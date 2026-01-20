"""
Parquet Converter - Taşınabilir Masaüstü Uygulaması
Excel dosyalarını hızlıca Parquet formatına dönüştürür
"""

import sys
import os
import json
import traceback
import logging
from datetime import datetime
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QProgressBar, QFileDialog, QScrollArea,
    QFrame, QSizePolicy, QMessageBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QMimeData
from PyQt6.QtGui import QDragEnterEvent, QDropEvent, QFont, QPalette, QColor

from converter import convert_excel_to_parquet, format_size, format_time, ConversionResult

# Uygulama dizini
APP_DIR = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(APP_DIR, "history.json")

# Loglama
logging.basicConfig(
    filename=os.path.join(APP_DIR, "app.log"),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ConversionWorker(QThread):
    """Arka planda dönüştürme işlemi yapan worker thread"""
    progress = pyqtSignal(int)
    finished = pyqtSignal(object)
    
    def __init__(self, input_path: str, output_path: str = None):
        super().__init__()
        self.input_path = input_path
        self.output_path = output_path
        self._cancelled = False
    
    def cancel(self):
        self._cancelled = True
    
    def run(self):
        if self._cancelled:
            return
        result = convert_excel_to_parquet(
            self.input_path,
            self.output_path,
            lambda p: self.progress.emit(p)
        )
        self.finished.emit(result)

class FileItem(QFrame):
    """Kuyrukta görünen dosya öğesi"""
    cancelled = pyqtSignal(object)
    
    def __init__(self, filename: str, filesize: int, parent=None):
        super().__init__(parent)
        self.filename = filename
        self.filesize = filesize
        self.result: ConversionResult = None
        self.worker: ConversionWorker = None
        self.setup_ui()
    
    def setup_ui(self):
        self.setStyleSheet("""
            FileItem {
                background-color: #1e293b;
                border: 1px solid #334155;
                border-radius: 10px;
                padding: 12px;
                margin: 4px 0;
            }
        """)
        
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(8)
        main_layout.setContentsMargins(12, 12, 12, 12)
        
        # Üst satır: Dosya adı ve kontrol butonları
        top_row = QHBoxLayout()
        
        # Dosya adı
        self.name_label = QLabel(os.path.basename(self.filename))
        self.name_label.setStyleSheet("font-weight: 600; font-size: 14px; color: #f1f5f9;")
        self.name_label.setWordWrap(True)
        top_row.addWidget(self.name_label, stretch=1)
        
        # İptal butonu
        self.cancel_btn = QPushButton("✕")
        self.cancel_btn.setFixedSize(28, 28)
        self.cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: transparent;
                border: 1px solid #475569;
                border-radius: 14px;
                color: #94a3b8;
                font-size: 12px;
            }
            QPushButton:hover {
                background-color: #dc2626;
                border-color: #dc2626;
                color: white;
            }
        """)
        self.cancel_btn.clicked.connect(self.on_cancel)
        top_row.addWidget(self.cancel_btn)
        
        main_layout.addLayout(top_row)
        
        # Dosya yolu (kısaltılmış)
        path_display = self.filename if len(self.filename) < 60 else "..." + self.filename[-57:]
        self.path_label = QLabel(path_display)
        self.path_label.setStyleSheet("color: #64748b; font-size: 11px;")
        self.path_label.setToolTip(self.filename)
        main_layout.addWidget(self.path_label)
        
        # Orta satır: Boyut, format ve durum
        info_row = QHBoxLayout()
        
        self.size_label = QLabel(format_size(self.filesize))
        self.size_label.setStyleSheet("color: #94a3b8; font-size: 12px;")
        info_row.addWidget(self.size_label)
        
        # Format göstergesi
        ext = os.path.splitext(self.filename)[1].upper()
        self.format_label = QLabel(f"{ext} → .PARQUET")
        self.format_label.setStyleSheet("""
            background-color: #3b82f6;
            color: white;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 10px;
            font-weight: 600;
        """)
        info_row.addWidget(self.format_label)
        
        info_row.addStretch()
        
        self.status_label = QLabel("Bekliyor...")
        self.status_label.setStyleSheet("""
            background-color: rgba(234, 179, 8, 0.2);
            color: #fbbf24;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 600;
        """)
        info_row.addWidget(self.status_label)
        
        main_layout.addLayout(info_row)
        
        # İlerleme çubuğu
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(6)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                background-color: #334155;
                border-radius: 3px;
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #6366f1, stop:1 #a855f7);
                border-radius: 3px;
            }
        """)
        self.progress_bar.hide()
        main_layout.addWidget(self.progress_bar)
        
        # Alt satır: Aksiyon butonları
        self.action_row = QHBoxLayout()
        self.action_row.addStretch()
        
        # Klasörü aç butonu
        self.open_folder_btn = QPushButton("📂 Klasörü Aç")
        self.open_folder_btn.setStyleSheet("""
            QPushButton {
                background-color: #059669;
                color: white;
                border: none;
                padding: 6px 14px;
                border-radius: 6px;
                font-weight: 600;
                font-size: 12px;
            }
            QPushButton:hover {
                background-color: #047857;
            }
        """)
        self.open_folder_btn.hide()
        self.open_folder_btn.clicked.connect(self.open_output_folder)
        self.action_row.addWidget(self.open_folder_btn)
        
        main_layout.addLayout(self.action_row)
    
    def on_cancel(self):
        if self.worker:
            self.worker.cancel()
        self.cancelled.emit(self)
    
    def set_converting(self):
        self.status_label.setText("Dönüştürülüyor...")
        self.status_label.setStyleSheet("""
            background-color: rgba(59, 130, 246, 0.2);
            color: #60a5fa;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 600;
        """)
        self.progress_bar.show()
        self.cancel_btn.hide()
    
    def set_progress(self, value: int):
        self.progress_bar.setValue(value)
    
    def set_done(self, result: ConversionResult):
        self.result = result
        self.progress_bar.setValue(100)
        self.cancel_btn.hide()
        
        if result.success:
            self.status_label.setText(f"✓ Tamamlandı ({format_time(result.elapsed)})")
            self.status_label.setStyleSheet("""
                background-color: rgba(34, 197, 94, 0.2);
                color: #4ade80;
                padding: 3px 10px;
                border-radius: 12px;
                font-size: 11px;
                font-weight: 600;
            """)
            self.size_label.setText(f"{format_size(result.input_size)} → {format_size(result.output_size)}")
            self.open_folder_btn.show()
            self.progress_bar.hide()
        else:
            self.status_label.setText(f"✗ Hata")
            self.status_label.setStyleSheet("""
                background-color: rgba(239, 68, 68, 0.2);
                color: #f87171;
                padding: 3px 10px;
                border-radius: 12px;
                font-size: 11px;
                font-weight: 600;
            """)
            self.path_label.setText(f"Hata: {result.error}")
            self.path_label.setStyleSheet("color: #f87171; font-size: 11px;")
            self.progress_bar.hide()
    
    def open_output_folder(self):
        if self.result and self.result.output_file:
            folder = os.path.dirname(self.result.output_file)
            os.system(f'xdg-open "{folder}"')

class DropZone(QFrame):
    """Sürükle-bırak alanı"""
    files_dropped = pyqtSignal(list)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setup_ui()
    
    def setup_ui(self):
        self.setMinimumHeight(140)
        self.setStyleSheet("""
            DropZone {
                background-color: #0f172a;
                border: 2px dashed #334155;
                border-radius: 16px;
            }
            DropZone:hover {
                border-color: #6366f1;
                background-color: rgba(99, 102, 241, 0.05);
            }
        """)
        
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        icon_label = QLabel("📁")
        icon_label.setStyleSheet("font-size: 42px; background: transparent;")
        icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(icon_label)
        
        text_label = QLabel("Excel dosyalarını sürükleyin veya seçin")
        text_label.setStyleSheet("color: #64748b; font-size: 13px; background: transparent;")
        text_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(text_label)
        
        self.browse_btn = QPushButton("Dosya Seç")
        self.browse_btn.setStyleSheet("""
            QPushButton {
                background-color: #6366f1;
                color: white;
                border: none;
                padding: 10px 28px;
                border-radius: 8px;
                font-weight: 600;
                font-size: 13px;
                margin-top: 8px;
            }
            QPushButton:hover {
                background-color: #4f46e5;
            }
        """)
        self.browse_btn.clicked.connect(self.browse_files)
        layout.addWidget(self.browse_btn, alignment=Qt.AlignmentFlag.AlignCenter)
    
    def browse_files(self):
        files, _ = QFileDialog.getOpenFileNames(
            self, "Excel Dosyaları Seç", "",
            "Excel Dosyaları (*.xlsx *.xls);;Tüm Dosyalar (*)"
        )
        if files:
            self.files_dropped.emit(files)
    
    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
            self.setStyleSheet("""
                DropZone {
                    background-color: rgba(99, 102, 241, 0.1);
                    border: 2px dashed #6366f1;
                    border-radius: 16px;
                }
            """)
    
    def dragLeaveEvent(self, event):
        self.setup_ui()
    
    def dropEvent(self, event: QDropEvent):
        files = []
        for url in event.mimeData().urls():
            path = url.toLocalFile()
            if path.endswith(('.xlsx', '.xls')):
                files.append(path)
        
        if files:
            self.files_dropped.emit(files)
        
        self.setup_ui()

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.queue = []
        self.current_worker = None
        self.processing = False
        self.history = []
        self.setup_ui()
        self.load_history()
    
    def setup_ui(self):
        self.setWindowTitle("Parquet Converter")
        self.setMinimumSize(650, 550)
        
        # Tamamen koyu tema
        self.setStyleSheet("""
            QMainWindow {
                background-color: #0f172a;
            }
            QWidget {
                background-color: transparent;
                color: #e2e8f0;
            }
            QScrollArea {
                border: none;
                background-color: transparent;
            }
            QScrollBar:vertical {
                background: #1e293b;
                width: 8px;
                border-radius: 4px;
            }
            QScrollBar::handle:vertical {
                background: #475569;
                border-radius: 4px;
                min-height: 20px;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
        """)
        
        central_widget = QWidget()
        central_widget.setStyleSheet("background-color: #0f172a;")
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(28, 28, 28, 28)
        main_layout.setSpacing(18)
        
        # Başlık
        title = QLabel("Parquet Converter")
        title.setStyleSheet("""
            font-size: 28px;
            font-weight: 700;
            color: #a78bfa;
            background: transparent;
        """)
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(title)
        
        subtitle = QLabel("Excel → Parquet dönüştürme (Power BI uyumlu)")
        subtitle.setStyleSheet("color: #64748b; font-size: 13px; background: transparent;")
        subtitle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(subtitle)
        
        # Sürükle-bırak alanı
        self.drop_zone = DropZone()
        self.drop_zone.files_dropped.connect(self.add_files)
        main_layout.addWidget(self.drop_zone)
        
        # Kuyruk başlığı
        queue_header = QHBoxLayout()
        queue_label = QLabel("İşlem Kuyruğu")
        queue_label.setStyleSheet("color: #a78bfa; font-size: 15px; font-weight: 600; background: transparent;")
        queue_header.addWidget(queue_label)
        
        queue_header.addStretch()
        
        self.clear_btn = QPushButton("Tümünü Temizle")
        self.clear_btn.setStyleSheet("""
            QPushButton {
                background-color: transparent;
                border: 1px solid #475569;
                color: #94a3b8;
                padding: 5px 12px;
                border-radius: 6px;
                font-size: 11px;
            }
            QPushButton:hover {
                border-color: #dc2626;
                color: #f87171;
            }
        """)
        self.clear_btn.clicked.connect(self.clear_queue)
        self.clear_btn.hide()
        queue_header.addWidget(self.clear_btn)
        
        main_layout.addLayout(queue_header)
        
        # Kuyruk scroll alanı
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setStyleSheet("background-color: transparent;")
        
        self.queue_widget = QWidget()
        self.queue_widget.setStyleSheet("background-color: transparent;")
        self.queue_layout = QVBoxLayout(self.queue_widget)
        self.queue_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.queue_layout.setSpacing(10)
        self.queue_layout.setContentsMargins(0, 0, 0, 0)
        
        scroll.setWidget(self.queue_widget)
        main_layout.addWidget(scroll, stretch=1)
        
        # Boş kuyruk mesajı
        self.empty_label = QLabel("Henüz dosya eklenmedi")
        self.empty_label.setStyleSheet("color: #475569; font-size: 13px; background: transparent;")
        self.empty_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.queue_layout.addWidget(self.empty_label)
    
    def add_files(self, files: list):
        self.empty_label.hide()
        self.clear_btn.show()
        
        for filepath in files:
            filesize = os.path.getsize(filepath)
            item = FileItem(filepath, filesize)
            item.cancelled.connect(self.remove_from_queue)
            self.queue.append(item)
            self.queue_layout.addWidget(item)
        
        self.process_queue()
    
    def remove_from_queue(self, item: FileItem):
        if item in self.queue:
            self.queue.remove(item)
            item.deleteLater()
        
        if not self.queue:
            self.empty_label.show()
            self.clear_btn.hide()
    
    def clear_queue(self):
        for item in self.queue[:]:
            if item.result is not None or not hasattr(item, 'processing'):
                self.queue.remove(item)
                item.deleteLater()
        
        if not self.queue:
            self.empty_label.show()
            self.clear_btn.hide()
    
    def process_queue(self):
        if self.processing:
            return
        
        # Bekleyen dosya bul
        pending_item = None
        for item in self.queue:
            if item.result is None and not hasattr(item, 'processing'):
                pending_item = item
                break
        
        if not pending_item:
            return
        
        self.processing = True
        pending_item.processing = True
        pending_item.set_converting()
        
        # Worker başlat
        self.current_worker = ConversionWorker(pending_item.filename)
        pending_item.worker = self.current_worker
        self.current_worker.progress.connect(pending_item.set_progress)
        self.current_worker.finished.connect(lambda r: self.on_conversion_done(pending_item, r))
        self.current_worker.start()
    
    def on_conversion_done(self, item: FileItem, result: ConversionResult):
        item.set_done(result)
        self.save_to_history(result)
        
        self.processing = False
        self.process_queue()
    
    def load_history(self):
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                    self.history = json.load(f)
            except:
                self.history = []
    
    def save_to_history(self, result: ConversionResult):
        if result.success:
            entry = {
                "input_file": result.input_file,
                "output_file": result.output_file,
                "input_size": result.input_size,
                "output_size": result.output_size,
                "elapsed": result.elapsed,
                "converted_at": datetime.now().isoformat()
            }
            self.history.insert(0, entry)
            self.history = self.history[:50]
            
            try:
                with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
                    json.dump(self.history, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logging.error(f"Geçmiş kaydedilemedi: {e}")

def main():
    try:
        logging.info("Uygulama başlatılıyor...")
        app = QApplication(sys.argv)
        
        font = QFont("Segoe UI", 10)
        app.setFont(font)
        
        window = MainWindow()
        window.show()
        
        logging.info("Uygulama hazır.")
        sys.exit(app.exec())
        
    except Exception as e:
        error_msg = f"Kritik hata: {str(e)}\n{traceback.format_exc()}"
        logging.error(error_msg)
        print(error_msg, file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
