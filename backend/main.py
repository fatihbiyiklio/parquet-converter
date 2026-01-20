import pandas as pd
import os
import json
import time
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import shutil
import uuid

app = FastAPI(title="Excel to Parquet Converter")

# CORS ayarları
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
OUT_DIR = "converted"
HISTORY_FILE = "history.json"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def save_history(history):
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

@app.get("/history")
async def get_history():
    return load_history()

@app.post("/convert")
async def convert_excel_to_parquet(file: UploadFile = File(...)):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Sadece Excel dosyaları (.xlsx, .xls) yüklenebilir.")
    
    file_id = str(uuid.uuid4())
    input_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
    output_filename = os.path.splitext(file.filename)[0] + ".parquet"
    output_path = os.path.join(OUT_DIR, f"{file_id}_{output_filename}")
    
    start_time = time.time()
    
    try:
        # Dosyayı kaydet
        with open(input_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        file_size = os.path.getsize(input_path)
        
        # Dönüştürme işlemi
        df = pd.read_excel(input_path)
        df.to_parquet(output_path, engine='pyarrow', index=False)
        
        elapsed_time = time.time() - start_time
        output_size = os.path.getsize(output_path)
        
        # Excel dosyasını sil
        os.remove(input_path)
        
        # Geçmişe ekle
        history = load_history()
        history_entry = {
            "id": file_id,
            "original_name": file.filename,
            "converted_name": output_filename,
            "original_size": file_size,
            "converted_size": output_size,
            "elapsed_time": round(elapsed_time, 2),
            "converted_at": datetime.now().isoformat(),
            "download_url": f"/download/{file_id}"
        }
        history.insert(0, history_entry)
        # Son 50 kayıt sakla
        history = history[:50]
        save_history(history)
        
        return {
            "id": file_id,
            "original_name": file.filename,
            "converted_name": output_filename,
            "original_size": file_size,
            "converted_size": output_size,
            "elapsed_time": round(elapsed_time, 2),
            "download_url": f"/download/{file_id}"
        }
    except Exception as e:
        # Hata durumunda Excel dosyasını da temizle
        if os.path.exists(input_path):
            os.remove(input_path)
        raise HTTPException(status_code=500, detail=f"Dönüştürme hatası: {str(e)}")

@app.get("/download/{file_id}")
async def download_file(file_id: str):
    # converted klasöründe bu ID ile başlayan dosyayı bul
    files = [f for f in os.listdir(OUT_DIR) if f.startswith(file_id)]
    if not files:
        raise HTTPException(status_code=404, detail="Dosya bulunamadı veya süresi dolmuş.")
    
    target_path = os.path.join(OUT_DIR, files[0])
    return FileResponse(
        path=target_path,
        filename=files[0].split('_', 1)[1],
        media_type='application/octet-stream'
    )

@app.delete("/history/{file_id}")
async def delete_history_item(file_id: str):
    history = load_history()
    history = [h for h in history if h['id'] != file_id]
    save_history(history)
    
    # İlgili parquet dosyasını da sil
    files = [f for f in os.listdir(OUT_DIR) if f.startswith(file_id)]
    for f in files:
        os.remove(os.path.join(OUT_DIR, f))
    
    return {"message": "Silindi"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
