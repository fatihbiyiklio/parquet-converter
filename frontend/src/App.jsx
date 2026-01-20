import React, { useState, useEffect, useRef } from 'react';
import './index.css';

const API_BASE = 'http://localhost:8000';

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatTime(seconds) {
  if (seconds < 60) return `${Math.round(seconds)}sn`;
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  return `${mins}dk ${secs}sn`;
}

function App() {
  const [queue, setQueue] = useState([]);
  const [history, setHistory] = useState([]);
  const [showHistory, setShowHistory] = useState(false);
  const processingRef = useRef(false);

  useEffect(() => {
    fetchHistory();
  }, []);

  const fetchHistory = async () => {
    try {
      const res = await fetch(`${API_BASE}/history`);
      if (res.ok) {
        const data = await res.json();
        setHistory(data);
      }
    } catch (err) {
      console.error('Geçmiş yüklenemedi:', err);
    }
  };

  const handleFileUpload = async (event) => {
    const files = Array.from(event.target.files);
    if (files.length === 0) return;

    const newItems = files.map(file => ({
      id: Math.random().toString(36).substr(2, 9),
      file: file,
      name: file.name,
      size: file.size,
      status: 'pending',
      progress: 0,
      elapsedTime: null,
      estimatedTime: null,
      downloadUrl: null,
      error: null
    }));

    setQueue(prev => [...prev, ...newItems]);

    // Dosya seçimini sıfırla
    event.target.value = '';
  };

  useEffect(() => {
    processQueue();
  }, [queue]);

  const processQueue = async () => {
    if (processingRef.current) return;

    const pendingItem = queue.find(item => item.status === 'pending');
    if (!pendingItem) return;

    processingRef.current = true;

    const startTime = Date.now();
    updateItem(pendingItem.id, { status: 'converting', progress: 10 });

    // Tahmini süre hesaplama (dosya boyutuna göre)
    const estimatedSeconds = Math.max(1, pendingItem.size / (1024 * 1024) * 2);
    updateItem(pendingItem.id, { estimatedTime: estimatedSeconds });

    // Simüle edilmiş ilerleme - geçen süreye orantılı
    const progressInterval = setInterval(() => {
      setQueue(prev => prev.map(item => {
        if (item.id === pendingItem.id && item.status === 'converting' && item.progress < 90) {
          const elapsed = (Date.now() - startTime) / 1000;
          const progressPercent = Math.min(90, 10 + (elapsed / estimatedSeconds) * 80);
          const remaining = Math.max(0, estimatedSeconds - elapsed);
          return {
            ...item,
            progress: progressPercent,
            elapsedTime: elapsed,
            estimatedTime: remaining
          };
        }
        return item;
      }));
    }, 100);

    const formData = new FormData();
    formData.append('file', pendingItem.file);

    try {
      const response = await fetch(`${API_BASE}/convert`, {
        method: 'POST',
        body: formData,
      });

      clearInterval(progressInterval);

      if (!response.ok) {
        const err = await response.json();
        throw new Error(err.detail || 'Dönüştürme hatası');
      }

      const data = await response.json();
      const totalTime = (Date.now() - startTime) / 1000;

      updateItem(pendingItem.id, {
        status: 'done',
        progress: 100,
        downloadUrl: data.download_url,
        elapsedTime: totalTime,
        estimatedTime: 0,
        convertedSize: data.converted_size
      });

      fetchHistory();
    } catch (err) {
      clearInterval(progressInterval);
      updateItem(pendingItem.id, {
        status: 'error',
        error: err.message,
        progress: 0
      });
    }

    processingRef.current = false;
  };

  const updateItem = (id, updates) => {
    setQueue(prev => prev.map(item =>
      item.id === id ? { ...item, ...updates } : item
    ));
  };

  const deleteHistoryItem = async (fileId) => {
    try {
      await fetch(`${API_BASE}/history/${fileId}`, { method: 'DELETE' });
      fetchHistory();
    } catch (err) {
      console.error('Silme hatası:', err);
    }
  };

  const clearCompleted = () => {
    setQueue(prev => prev.filter(item => item.status !== 'done' && item.status !== 'error'));
  };

  return (
    <div className="app-container">
      <h1>Parquet Converter</h1>
      <p className="subtitle">Excel dosyalarınızı Power BI için Parquet formatına dönüştürün</p>

      <label className="upload-zone">
        <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
        </svg>
        <p>Dosyaları buraya sürükleyin veya seçmek için tıklayın</p>
        <input type="file" multiple accept=".xlsx, .xls" onChange={handleFileUpload} />
      </label>

      {queue.length > 0 && (
        <div className="queue-container">
          <div className="section-header">
            <h2>İşlem Kuyruğu</h2>
            {queue.some(i => i.status === 'done' || i.status === 'error') && (
              <button className="clear-btn" onClick={clearCompleted}>Tamamlananları Temizle</button>
            )}
          </div>
          {queue.map(item => (
            <div key={item.id} className="queue-item">
              <div className="file-info">
                <span className="file-name">{item.name}</span>
                <span className="file-meta">{formatBytes(item.size)}</span>

                {item.status === 'converting' && (
                  <div className="progress-container">
                    <div className="progress-bar">
                      <div className="progress-fill" style={{ width: `${item.progress}%` }}></div>
                    </div>
                    <div className="progress-info">
                      <span>%{Math.round(item.progress)}</span>
                      {item.estimatedTime !== null && item.estimatedTime > 0 && (
                        <span className="time-remaining">~{formatTime(item.estimatedTime)} kaldı</span>
                      )}
                    </div>
                  </div>
                )}

                <span className="file-status">
                  {item.status === 'pending' && <span className="status-badge status-pending">Bekliyor...</span>}
                  {item.status === 'converting' && <span className="status-badge status-converting">Dönüştürülüyor...</span>}
                  {item.status === 'done' && (
                    <span className="status-badge status-done">
                      Tamamlandı ({formatTime(item.elapsedTime)})
                    </span>
                  )}
                  {item.status === 'error' && <span className="status-badge status-error">Hata: {item.error}</span>}
                </span>
              </div>
              {item.status === 'done' && (
                <a href={`${API_BASE}${item.downloadUrl}`} className="download-btn" download>
                  İndir
                </a>
              )}
            </div>
          ))}
        </div>
      )}

      <div className="history-section">
        <button className="history-toggle" onClick={() => setShowHistory(!showHistory)}>
          {showHistory ? '▲ Geçmişi Gizle' : '▼ Dönüştürme Geçmişi'} ({history.length})
        </button>

        {showHistory && history.length > 0 && (
          <div className="history-list">
            {history.map(item => (
              <div key={item.id} className="history-item">
                <div className="history-info">
                  <span className="history-name">{item.original_name}</span>
                  <span className="history-meta">
                    {formatBytes(item.original_size)} → {formatBytes(item.converted_size)} | {formatTime(item.elapsed_time)} | {new Date(item.converted_at).toLocaleDateString('tr-TR')}
                  </span>
                </div>
                <div className="history-actions">
                  <a href={`${API_BASE}${item.download_url}`} className="download-btn small" download>İndir</a>
                  <button className="delete-btn" onClick={() => deleteHistoryItem(item.id)}>✕</button>
                </div>
              </div>
            ))}
          </div>
        )}

        {showHistory && history.length === 0 && (
          <p className="no-history">Henüz dönüştürme geçmişi yok.</p>
        )}
      </div>
    </div>
  );
}

export default App;
