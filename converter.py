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
import numpy as np
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
    - Tipleri korumaya çalışır (String'e zorlamaz)
    - Null değerleri PyArrow'un anlayacağı formatta bırakır (None, NaN, NaT)
    """
    # Kolon isimlerini string yap (zorunlu)
    df.columns = df.columns.astype(str)
    
    for col in df.columns:
        # object/mixed tipleri analiz et ve mümkünse dönüştür
        if df[col].dtype == 'object':
            try:
                # Sayısal veri var mı diye bak (coerce hataları NaN yapar)
                df_numeric = pd.to_numeric(df[col], errors='coerce')
                # Eğer verinin çoğu sayıysa ve sadece az sayıda NaN varsa, değişim mantıklıdır.
                # Ancak burada basitlik adına: Eğer orijinalde sayısal olmayan değer neredeyse yoksa dönüştür.
                # Ama güvenli olması için object olarak bırakmak çoğu zaman daha iyidir, 
                # fakat Power BI için explicit type conversion gerekebilir.
                pass 
            except:
                pass

        # Integer sütunları - nullable int desteği için Int64'e çevir
        if pd.api.types.is_integer_dtype(df[col]):
            # Zaten nullable değilse ve null yoksa dokunma, ama standart olması için Int64 yapabiliriz
            df[col] = df[col].astype('Int64')
        
        # Float olup aslında integer olanları (örn: 12.0) Int64 yapmayı deneyebiliriz?
        # Şimdilik float kalsın, Power BI float sever.
        
        # Tarih/Zaman
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # NaT zaten Parquet null ile uyumludur
            pass
            
    return df


def create_powerbi_compatible_schema(df: pd.DataFrame) -> pa.Schema:
    """
    Power BI ile uyumlu PyArrow şeması oluşturur.
    Tüm alanları nullable yapar.
    """
    fields = []
    
    for col in df.columns:
        dtype = df[col].dtype
        
        pa_type = None
        
        if pd.api.types.is_integer_dtype(dtype):
            pa_type = pa.int64()
        elif pd.api.types.is_float_dtype(dtype):
            pa_type = pa.float64()
        elif pd.api.types.is_bool_dtype(dtype):
            pa_type = pa.bool_()
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            pa_type = pa.timestamp('ms') # Power BI genelde ms veya us sever
        elif pd.api.types.is_string_dtype(dtype):
            pa_type = pa.string()
        else:
            # Fallback
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
        
        if progress_callback:
            progress_callback(50)
        
        # Power BI uyumlu şema oluştur
        try:
            # Basitçe pyarrow'un pandas'tan şema çıkarmasına izin verelim ama nullable olduğundan emin olalım
            # Ancak manuel şema oluşturmak en güvenlisidir çünkü Power BI sütun tipleri konusunda hassastır.
            # Şimdilik pa.Table.from_pandas'a bırakıp, sonra şemayı modifiye etmeyi deneyelim veya
            # df'i temizledikten sonra direkt dönüştürelim.
            
            # En temiz yöntem: df zaten temizlendi (clean_dataframe_for_powerbi ile int64 vb yapıldı)
            # PyArrow'un kendi tip çıkarımını kullanalım, genellikle oldukça iyidir.
            
            table = pa.Table.from_pandas(df_pandas, preserve_index=False)
            
            # Şemadaki tüm alanları nullable olarak işaretle (Power BI hatasını önlemek için kritik)
            new_fields = []
            for field in table.schema:
                new_fields.append(field.with_nullable(True))
            
            new_schema = pa.schema(new_fields)
            table = table.cast(new_schema)
            
        except Exception as e:
            # Fallback: String'e çevirip öyle dene (eski yöntem, ama sadece hata durumunda)
            df_str = df_pandas.astype(str)
            table = pa.Table.from_pandas(df_str, preserve_index=False)
        
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
