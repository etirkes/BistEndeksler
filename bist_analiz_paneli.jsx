import React, { useState, useEffect, useMemo } from 'react';
// Paket yöneticisi olmayan ortamlar için import'u CDN (esm.sh) üzerinden yapıyoruz.
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'; 
import { ArrowUp, ArrowDown, Search, ChevronRight, BarChart2, ArrowLeft, RefreshCw, AlertCircle } from 'lucide-react';

// --- SUPABASE CONFIGURATION ---
// Bu bilgileri Supabase proje ayarlarından alıp buraya yapıştırın.
const SUPABASE_URL = 'https://YOUR_PROJECT_ID.supabase.co';
const SUPABASE_ANON_KEY = 'YOUR_ANON_KEY';

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// --- MOCK DATA GENERATORS (FALLBACK) ---
const generateMockData = () => {
  const INDICES_CONFIG = [
    { code: 'XU100', name: 'BIST 100', category: 'Genel' },
    { code: 'XBANK', name: 'Bankacılık', category: 'Sektör' },
    { code: 'XUSIN', name: 'Sınai', category: 'Sektör' },
    { code: 'XUTEK', name: 'Teknoloji', category: 'Sektör' },
    { code: 'XULAS', name: 'Ulaştırma', category: 'Sektör' },
    { code: 'XGIDA', name: 'Gıda İçecek', category: 'Sektör' },
  ];

  const indices = INDICES_CONFIG.map(idx => ({
    ...idx,
    last_price: (Math.random() * 5000 + 2000).toFixed(2),
    change1d: (Math.random() * 6 - 3),
    change1w: (Math.random() * 10 - 5),
    change1m: (Math.random() * 15 - 7),
    change3m: (Math.random() * 20 - 10),
    volume: (Math.random() * 100 + 10).toFixed(1) + 'M',
    updated_at: new Date().toISOString()
  }));

  const stocks = [];
  indices.forEach(idx => {
    for (let i = 0; i < 5; i++) {
        stocks.push({
            symbol: `${idx.code}_HISSE${i+1}`,
            parent_index: idx.code, // Mock veride tek index kalabilir
            name: `${idx.name} Şirket ${i+1}`,
            price: (Math.random() * 100 + 10).toFixed(2),
            change1d: idx.change1d + (Math.random() * 2 - 1),
            change1w: idx.change1w + (Math.random() * 4 - 2),
            change1m: idx.change1m + (Math.random() * 6 - 3),
            change3m: idx.change3m + (Math.random() * 8 - 4),
            updated_at: new Date().toISOString()
        });
    }
  });

  return { indices, stocks };
};

// Yardımcı Fonksiyon: Renk belirleme
const getColorClass = (value) => {
  if (value > 0) return 'text-green-500';
  if (value < 0) return 'text-red-500';
  return 'text-gray-400';
};

const formatPercent = (val) => {
    if (val === null || val === undefined) return '-';
    return `${val > 0 ? '+' : ''}${Number(val).toFixed(2)}%`;
};

export default function BistAnalyzer() {
  const [view, setView] = useState('indices'); // 'indices' or 'stocks'
  const [selectedIndex, setSelectedIndex] = useState(null);
  const [sortConfig, setSortConfig] = useState({ key: 'change1w', direction: 'desc' });
  const [searchTerm, setSearchTerm] = useState('');
  
  // Veri State'leri
  const [indicesData, setIndicesData] = useState([]);
  const [stocksData, setStocksData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [isDemoMode, setIsDemoMode] = useState(false);

  // Veri Çekme Fonksiyonu
  const fetchData = async () => {
    setLoading(true);
    setError(null);
    setIsDemoMode(false);
    
    try {
      // API Anahtarı kontrolü
      if (SUPABASE_URL.includes('YOUR_PROJECT_ID')) {
        throw new Error("API keys not set");
      }

      // 1. Endeksleri Çek (bist_indices)
      const { data: indices, error: indicesError } = await supabase
        .from('bist_indices')
        .select('*');
      
      if (indicesError) throw indicesError;

      // 2. Hisseleri Çek (bist_stocks)
      const { data: stocks, error: stocksError } = await supabase
        .from('bist_stocks')
        .select('*');
        
      if (stocksError) throw stocksError;

      setIndicesData(indices || []);
      setStocksData(stocks || []);
      
      if (indices && indices.length > 0) {
        setLastUpdated(new Date(indices[0].updated_at));
      }

    } catch (err) {
      console.warn('Gerçek veri çekilemedi, demo moduna geçiliyor:', err.message);
      const { indices, stocks } = generateMockData();
      setIndicesData(indices);
      setStocksData(stocks);
      setLastUpdated(new Date());
      setIsDemoMode(true);
      
      if (!err.message.includes("API keys")) {
          setError(`Supabase hatası: ${err.message}. Demo veriler gösteriliyor.`);
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  // Endeks seçildiğinde filtreleme
  const handleIndexClick = (indexCode) => {
    setSelectedIndex(indicesData.find(i => i.code === indexCode));
    setView('stocks');
    setSortConfig({ key: 'change1w', direction: 'desc' });
  };

  // --- KRİTİK GÜNCELLEME BURADA ---
  // Hisselerin parent_index alanı artık "XU100,XULAS" gibi string formatında olabilir.
  // Bu yüzden tam eşleşme (===) yerine .includes() kullanıyoruz.
  const currentStocks = useMemo(() => {
    if (!selectedIndex) return [];
    return stocksData.filter(stock => 
        stock.parent_index && stock.parent_index.includes(selectedIndex.code)
    );
  }, [stocksData, selectedIndex]);

  // Sıralama Fonksiyonu
  const handleSort = (key) => {
    let direction = 'desc';
    if (sortConfig.key === key && sortConfig.direction === 'desc') {
      direction = 'asc';
    }
    setSortConfig({ key, direction });
  };

  const sortedIndices = useMemo(() => {
    let sortableItems = [...indicesData];
    if (searchTerm) {
      sortableItems = sortableItems.filter(item => 
        item.name?.toLowerCase().includes(searchTerm.toLowerCase()) || 
        item.code.toLowerCase().includes(searchTerm.toLowerCase())
      );
    }
    if (sortConfig !== null) {
      sortableItems.sort((a, b) => {
        const valA = Number(a[sortConfig.key]) || 0;
        const valB = Number(b[sortConfig.key]) || 0;
        if (valA < valB) return sortConfig.direction === 'asc' ? -1 : 1;
        if (valA > valB) return sortConfig.direction === 'asc' ? 1 : -1;
        return 0;
      });
    }
    return sortableItems;
  }, [indicesData, sortConfig, searchTerm]);

  const sortedStocks = useMemo(() => {
    let sortableItems = [...currentStocks];
    if (sortConfig !== null) {
      sortableItems.sort((a, b) => {
        const valA = Number(a[sortConfig.key]) || 0;
        const valB = Number(b[sortConfig.key]) || 0;
        if (valA < valB) return sortConfig.direction === 'asc' ? -1 : 1;
        if (valA > valB) return sortConfig.direction === 'asc' ? 1 : -1;
        return 0;
      });
    }
    return sortableItems;
  }, [currentStocks, sortConfig]);

  const TableHeader = ({ label, sortKey, align = 'right' }) => (
    <th 
      className={`px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider cursor-pointer hover:text-white transition-colors ${align === 'left' ? 'text-left' : 'text-right'}`}
      onClick={() => handleSort(sortKey)}
    >
      <div className={`flex items-center ${align === 'left' ? 'justify-start' : 'justify-end'} gap-1`}>
        {label}
        {sortConfig.key === sortKey && (
          sortConfig.direction === 'asc' ? <ArrowUp size={14} /> : <ArrowDown size={14} />
        )}
      </div>
    </th>
  );

  return (
    <div className="min-h-screen bg-slate-900 text-gray-100 font-sans selection:bg-blue-500 selection:text-white">
      {/* Header */}
      <header className="bg-slate-800 border-b border-slate-700 sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <BarChart2 className="text-blue-500" size={28} />
            <div className="flex flex-col">
                <h1 className="text-xl font-bold bg-gradient-to-r from-blue-400 to-indigo-400 bg-clip-text text-transparent">
                BIST Radar (Canlı)
                </h1>
                {isDemoMode && <span className="text-[10px] text-yellow-500 font-mono">DEMO MODU - API KEY GİRİLMELİ</span>}
            </div>
          </div>
          
          <div className="flex items-center gap-4 text-sm text-gray-400">
            <span className="hidden sm:inline">
                {lastUpdated ? `Güncelleme: ${lastUpdated.toLocaleTimeString()}` : 'Yükleniyor...'}
            </span>
            <button 
              onClick={fetchData}
              className={`p-2 bg-slate-700 hover:bg-slate-600 rounded-full transition-all ${loading ? 'animate-spin' : ''}`}
              title="Verileri Yenile"
            >
              <RefreshCw size={18} />
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        
        {/* Error / Demo Info State */}
        {error ? (
            <div className="mb-6 p-4 bg-red-900/50 border border-red-700 rounded-lg flex items-center gap-3 text-red-200">
                <AlertCircle size={24} />
                <div>
                    <p className="font-bold">Bağlantı Hatası</p>
                    <p className="text-sm">{error}</p>
                </div>
            </div>
        ) : isDemoMode && (
            <div className="mb-6 p-4 bg-blue-900/30 border border-blue-800 rounded-lg flex items-center gap-3 text-blue-200">
                <AlertCircle size={24} />
                <div>
                    <p className="font-bold">Kurulum Bekleniyor</p>
                    <p className="text-sm">Şu anda demo verileri görüntüleniyor. Gerçek verileri görmek için kodun içindeki `SUPABASE_URL` ve `SUPABASE_KEY` alanlarını güncelleyiniz.</p>
                </div>
            </div>
        )}

        {/* Navigation */}
        <div className="mb-6 flex items-center justify-between">
            <div className="flex items-center gap-2 text-lg">
                {view === 'stocks' ? (
                    <>
                        <button onClick={() => setView('indices')} className="text-gray-400 hover:text-white flex items-center gap-1">
                            <ArrowLeft size={18} /> Tüm Endeksler
                        </button>
                        <ChevronRight size={18} className="text-gray-600" />
                        <span className="font-semibold text-blue-400">{selectedIndex?.name} ({selectedIndex?.code})</span>
                    </>
                ) : (
                    <span className="font-semibold text-white">Sektör Rotasyonları</span>
                )}
            </div>

            {view === 'indices' && (
                <div className="relative">
                    <input 
                        type="text" 
                        placeholder="Endeks Ara..." 
                        className="bg-slate-800 border border-slate-700 text-white text-sm rounded-lg pl-10 pr-4 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500 w-64"
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                    />
                    <Search className="absolute left-3 top-2.5 text-gray-500" size={16} />
                </div>
            )}
        </div>

        {/* Table */}
        <div className="bg-slate-800 rounded-xl shadow-xl border border-slate-700 overflow-hidden min-h-[400px]">
            {loading && indicesData.length === 0 ? (
                <div className="flex items-center justify-center h-64 text-gray-400">
                    <div className="animate-spin mr-2"><RefreshCw/></div> Veriler yükleniyor...
                </div>
            ) : (
                <div className="overflow-x-auto">
                    <table className="w-full whitespace-nowrap">
                        <thead>
                            <tr className="bg-slate-900/50 border-b border-slate-700">
                                <TableHeader label={view === 'indices' ? "Endeks" : "Hisse"} sortKey={view === 'indices' ? "code" : "symbol"} align="left" />
                                {view === 'indices' && <TableHeader label="Kategori" sortKey="category" align="left" />}
                                <TableHeader label="Fiyat" sortKey={view === 'indices' ? "last_price" : "price"} />
                                <TableHeader label="Günlük %" sortKey="change1d" />
                                <TableHeader label="Haftalık %" sortKey="change1w" />
                                <TableHeader label="Aylık %" sortKey="change1m" />
                                <TableHeader label="3 Aylık %" sortKey="change3m" />
                                {view === 'indices' && <TableHeader label="Hacim" sortKey="volume" />}
                                {view === 'indices' && <th className="px-4 py-3 text-right text-xs font-semibold text-gray-400 uppercase">Detay</th>}
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-700">
                            {(view === 'indices' ? sortedIndices : sortedStocks).map((item, idx) => (
                                <tr 
                                    key={idx} 
                                    className={`hover:bg-slate-700/50 transition-colors ${view === 'indices' ? 'cursor-pointer' : ''}`}
                                    onClick={() => view === 'indices' && handleIndexClick(item.code)}
                                >
                                    <td className="px-4 py-4">
                                        <div className="flex items-center">
                                            <div className="ml-0">
                                                <div className="text-sm font-bold text-white">{item.code || item.symbol}</div>
                                                <div className="text-xs text-gray-400">{item.name}</div>
                                            </div>
                                        </div>
                                    </td>
                                    {view === 'indices' && (
                                        <td className="px-4 py-4 text-sm text-gray-300">
                                            <span className="px-2 py-1 rounded-full bg-slate-800 border border-slate-600 text-xs">
                                                {item.category}
                                            </span>
                                        </td>
                                    )}
                                    <td className="px-4 py-4 text-right text-sm font-mono text-white">
                                        {item.last_price || item.price}
                                    </td>
                                    <td className={`px-4 py-4 text-right text-sm font-mono font-medium ${getColorClass(item.change1d)}`}>
                                        {formatPercent(item.change1d)}
                                    </td>
                                    <td className={`px-4 py-4 text-right text-sm font-mono font-medium ${getColorClass(item.change1w)}`}>
                                        <div className="flex items-center justify-end gap-1">
                                            {formatPercent(item.change1w)}
                                            <div className={`w-1 h-4 rounded-full ${item.change1w > 0 ? 'bg-green-500/20' : 'bg-red-500/20'}`}>
                                                <div 
                                                    className={`w-full rounded-full ${item.change1w > 0 ? 'bg-green-500' : 'bg-red-500'}`} 
                                                    style={{ height: `${Math.min(Math.abs(item.change1w) * 5, 100)}%` }}
                                                />
                                            </div>
                                        </div>
                                    </td>
                                    <td className={`px-4 py-4 text-right text-sm font-mono font-medium ${getColorClass(item.change1m)}`}>
                                        {formatPercent(item.change1m)}
                                    </td>
                                    <td className={`px-4 py-4 text-right text-sm font-mono font-medium ${getColorClass(item.change3m)}`}>
                                        {formatPercent(item.change3m)}
                                    </td>
                                    {view === 'indices' && (
                                        <td className="px-4 py-4 text-right text-sm text-gray-400">
                                            {item.volume}
                                        </td>
                                    )}
                                    {view === 'indices' && (
                                        <td className="px-4 py-4 text-right">
                                            <ChevronRight size={18} className="text-gray-500 inline-block" />
                                        </td>
                                    )}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                     {((view === 'indices' && sortedIndices.length === 0) || (view === 'stocks' && sortedStocks.length === 0)) && !loading && (
                        <div className="p-8 text-center text-gray-400">
                            Veri bulunamadı.
                        </div>
                    )}
                </div>
            )}
        </div>
      </main>
    </div>
  );
}
