"""
Parquet Converter - Hızlı Excel → Parquet Dönüştürücü
Power BI uyumlu, tip güvenli dönüştürme motoru
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

def clean_dataframe_for_powerbi(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame'i Power BI uyumlu Parquet formatına hazırlar.
    - NaT, null, None değerleri boş string'e çevirir
    - Karışık tip sütunları string'e dönüştürür
    - inf değerlerini temizler
    """
    import numpy as np
    
    for col in df.columns:
        dtype = df[col].dtype
        
        # Datetime sütunları - NaT değerlerini boş string'e çevir
        if pd.api.types.is_datetime64_any_dtype(dtype):
            df[col] = df[col].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else ""
            )
            continue
        
        # Tamamen null olan sütunları boş string yap
        if df[col].isna().all():
            df[col] = ""
            continue
        
        # Float sütunları - inf ve NaN'ı boş string'e çevir
        if dtype in ['float64', 'float32']:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            # NaN'ları boş string'e çevirmek için object'e dönüştür
            df[col] = df[col].apply(lambda x: "" if pd.isna(x) else x)
        
        # Integer sütunları - nullable int'leri işle
        if pd.api.types.is_integer_dtype(dtype):
            if df[col].isna().any():
                df[col] = df[col].apply(lambda x: "" if pd.isna(x) else int(x))
        
        # Object tipi sütunları string'e dönüştür
        if dtype == 'object':
            df[col] = df[col].apply(
                lambda x: "" if pd.isna(x) or x is None or str(x).lower() in ['nan', 'none', 'nat'] else str(x)
            )
    
    # Tüm sütunları object/string tipine çevir (boş değerler için)
    for col in df.columns:
        df[col] = df[col].astype(str).replace('nan', '').replace('None', '').replace('NaT', '').replace('<NA>', '')
    
    return df


def create_powerbi_compatible_schema(df: pd.DataFrame) -> pa.Schema:
    """
    Power BI ile uyumlu PyArrow şeması oluşturur.
    Null değerli sütunlar için açık tip tanımı yapar.
    """
    fields = []
    
    for col in df.columns:
        dtype = df[col].dtype
        
        if dtype == 'int64':
            pa_type = pa.int64()
        elif dtype == 'int32':
            pa_type = pa.int32()
        elif dtype == 'float64':
            pa_type = pa.float64()
        elif dtype == 'float32':
            pa_type = pa.float32()
        elif dtype == 'bool':
            pa_type = pa.bool_()
        elif dtype == 'datetime64[ns]':
            pa_type = pa.timestamp('ns')
        elif dtype == 'object' or str(dtype) == 'string':
            pa_type = pa.string()
        else:
            # Bilinmeyen tipler için string kullan
            pa_type = pa.string()
        
        fields.append(pa.field(col, pa_type, nullable=True))
    
    return pa.schema(fields)

def convert_excel_to_parquet(
    input_path: str,
    output_path: str = None,
    progress_callback: Optional[Callable[[int], None]] = None
) -> ConversionResult:
    """
    Excel dosyasını Power BI uyumlu Parquet formatına dönüştürür.
    Tip güvenliği ve null işleme dahil.
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
        try:
            df_pandas = pd.read_excel(input_path, engine='openpyxl')
        except Exception:
            # xlrd ile eski Excel formatlarını dene
            df_pandas = pd.read_excel(input_path, engine='xlrd')
        
        if progress_callback:
            progress_callback(30)
        
        # DataFrame'i Power BI uyumlu hale getir
        df_pandas = clean_dataframe_for_powerbi(df_pandas)
        
        if progress_callback:
            progress_callback(50)
        
        # Açık şema ile PyArrow Table oluştur
        try:
            schema = create_powerbi_compatible_schema(df_pandas)
            
            # Sütun sütun array oluştur
            arrays = []
            for col in df_pandas.columns:
                col_data = df_pandas[col].tolist()
                try:
                    arr = pa.array(col_data)
                except Exception:
                    # String'e zorla
                    arr = pa.array([str(x) if x is not None and str(x) not in ['nan', 'None', 'NaN'] else "" for x in col_data], type=pa.string())
                arrays.append(arr)
            
            table = pa.table(dict(zip(df_pandas.columns, arrays)))
            
        except Exception as e:
            # Fallback: Basit dönüşüm
            table = pa.Table.from_pandas(df_pandas, preserve_index=False)
        
        if progress_callback:
            progress_callback(80)
        
        # Parquet olarak kaydet (Power BI uyumlu ayarlarla)
        pq.write_table(
            table, 
            output_path, 
            compression='snappy',
            use_dictionary=True,
            write_statistics=True
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
