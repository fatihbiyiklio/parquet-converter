# Parquet Converter

Excel dosyalarını hızlıca Parquet formatına dönüştüren modern masaüstü uygulaması.

## Özellikler
- 🚀 **Hızlı**: Polars + Pandas ile yüksek performanslı dönüştürme
- 🎨 **Modern UI**: PyQt6 ile koyu temalı sürükle-bırak arayüzü
- 📦 **Taşınabilir**: Tek dosyalık executable, kurulum gerektirmez
- 🔄 **Toplu İşlem**: Birden fazla dosyayı sıraya alıp işleme
- ✅ **Power BI Uyumlu**: Snappy sıkıştırmalı Parquet çıktısı

## İndirme

[Releases](../../releases) sayfasından işletim sisteminize uygun executable'ı indirin:
- **Windows**: `ParquetConverter.exe`
- **Linux**: `ParquetConverter`

## Kaynak Koddan Çalıştırma

```bash
# Sanal ortam oluştur
python -m venv venv
source venv/bin/activate  # Linux/macOS
# venv\Scripts\activate   # Windows

# Bağımlılıkları yükle
pip install -r requirements.txt

# Çalıştır
python main.py
```

## Build

```bash
# Executable oluştur
pip install pyinstaller
pyinstaller --onefile --windowed --name ParquetConverter main.py
```

## GitHub Actions

Bu repo, push veya tag oluşturma işlemlerinde otomatik olarak:
- Windows `.exe` 
- Linux binary

oluşturur. Tag formatı: `v1.0.0`

## Lisans

MIT
