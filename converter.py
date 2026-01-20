"""
Parquet Converter - Hızlı Excel → Parquet Dönüştürücü
Polars tabanlı yüksek performanslı dönüştürme motoru
"""

import polars as pl
import pandas as pd
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

def convert_excel_to_parquet(
    input_path: str,
    output_path: str = None,
    progress_callback: Optional[Callable[[int], None]] = None
) -> ConversionResult:
    """
    Excel dosyasını Parquet formatına dönüştürür.
    Pandas fallback ile maksimum uyumluluk sağlar.
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
        
        # Pandas ile Excel okuma (daha güvenilir, inf/nan işleme dahil)
        df_pandas = pd.read_excel(input_path)
        
        if progress_callback:
            progress_callback(40)
        
        # inf değerlerini NaN'a çevir
        import numpy as np
        df_pandas = df_pandas.replace([np.inf, -np.inf], np.nan)
        
        if progress_callback:
            progress_callback(60)
        
        # Polars'a dönüştür
        df = pl.from_pandas(df_pandas)
        
        if progress_callback:
            progress_callback(80)
        
        # Parquet olarak kaydet (Snappy sıkıştırma ile)
        df.write_parquet(
            output_path,
            compression="snappy",
            use_pyarrow=True
        )
        
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
