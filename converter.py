"""
Parquet Converter - Hızlı Excel → Parquet Dönüştürücü
Polars tabanlı yüksek performanslı dönüştürme motoru
"""

import polars as pl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Optional

class ConversionResult:
    def __init__(self, success: bool, input_file: str, output_file: str = None,
                 input_size: int = 0, output_size: int = 0, elapsed: float = 0,
                 error: str = None):
        self.success = success
        self.input_file = input_file
        self.output_file = output_file
        self.input_size = input_size
        self.output_size = output_size
        self.elapsed = elapsed
        self.error = error

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame'i Parquet uyumlu hale getirir.
    Karışık tip sütunları ve özel değerleri temizler.
    """
    import numpy as np
    
    for col in df.columns:
        # inf değerlerini NaN'a çevir
        if df[col].dtype in ['float64', 'float32']:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
        
        # object tipi sütunları string'e dönüştür (bytes vs int karışıklığını önler)
        if df[col].dtype == 'object':
            try:
                # Karışık tipleri string'e zorla
                df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and x is not None else None)
            except Exception:
                # Son çare: tüm sütunu string'e çevir
                df[col] = df[col].astype(str).replace('nan', None).replace('None', None)
    
    return df

def convert_excel_to_parquet(
    input_path: str,
    output_path: str = None,
    progress_callback: Optional[Callable[[int], None]] = None
) -> ConversionResult:
    """
    Excel dosyasını Parquet formatına dönüştürür.
    PyArrow ile doğrudan yazma - maksimum uyumluluk.
    """
    start_time = time.time()
    
    try:
        if not os.path.exists(input_path):
            return ConversionResult(False, input_path, error="Dosya bulunamadı")
        
        input_size = os.path.getsize(input_path)
        
        # Çıktı dosyası belirlenmemişse aynı dizinde .parquet olarak oluştur
        if output_path is None:
            base_name = os.path.splitext(input_path)[0]
            output_path = base_name + ".parquet"
        
        if progress_callback:
            progress_callback(10)
        
        # Pandas ile Excel okuma
        df_pandas = pd.read_excel(input_path, engine='openpyxl')
        
        if progress_callback:
            progress_callback(30)
        
        # DataFrame'i temizle
        df_pandas = clean_dataframe(df_pandas)
        
        if progress_callback:
            progress_callback(50)
        
        # PyArrow Table'a dönüştür (daha güvenilir)
        try:
            table = pa.Table.from_pandas(df_pandas, preserve_index=False)
        except Exception as e:
            # Fallback: Sütun sütun dönüştür
            columns = []
            for col in df_pandas.columns:
                try:
                    arr = pa.array(df_pandas[col].tolist())
                except Exception:
                    # String'e zorla
                    arr = pa.array([str(x) if pd.notna(x) else None for x in df_pandas[col]])
                columns.append(arr)
            
            table = pa.table(dict(zip(df_pandas.columns, columns)))
        
        if progress_callback:
            progress_callback(80)
        
        # Parquet olarak kaydet
        pq.write_table(table, output_path, compression='snappy')
        
        if progress_callback:
            progress_callback(100)
        
        elapsed = time.time() - start_time
        output_size = os.path.getsize(output_path)
        
        return ConversionResult(
            success=True,
            input_file=input_path,
            output_file=output_path,
            input_size=input_size,
            output_size=output_size,
            elapsed=elapsed
        )
        
    except Exception as e:
        elapsed = time.time() - start_time
        return ConversionResult(
            success=False,
            input_file=input_path,
            elapsed=elapsed,
            error=str(e)
        )

def convert_multiple(
    input_files: list[str],
    output_dir: str = None,
    max_workers: int = None,
    progress_callback: Optional[Callable[[str, int], None]] = None
) -> list[ConversionResult]:
    """
    Birden fazla Excel dosyasını paralel olarak dönüştürür.
    CPU çekirdek sayısına göre otomatik ölçeklenir.
    """
    if max_workers is None:
        max_workers = min(os.cpu_count() or 4, len(input_files))
    
    results = []
    
    def process_file(input_path: str) -> ConversionResult:
        if output_dir:
            filename = os.path.basename(input_path)
            base_name = os.path.splitext(filename)[0]
            output_path = os.path.join(output_dir, base_name + ".parquet")
        else:
            output_path = None
        
        def file_progress(percent):
            if progress_callback:
                progress_callback(input_path, percent)
        
        return convert_excel_to_parquet(input_path, output_path, file_progress)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(process_file, f): f for f in input_files}
        
        for future in as_completed(future_to_file):
            result = future.result()
            results.append(result)
    
    return results

def format_size(bytes_size: int) -> str:
    """Byte değerini okunabilir formata çevirir"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.1f} TB"

def format_time(seconds: float) -> str:
    """Saniyeyi okunabilir formata çevirir"""
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}sn"
    else:
        mins = int(seconds // 60)
        secs = seconds % 60
        return f"{mins}dk {secs:.0f}sn"
