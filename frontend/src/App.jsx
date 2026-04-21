// App.jsx — Production HFT Dashboard v3 (Refactored)
import React, {
  useState, useEffect, useRef, useCallback, useMemo, createContext, useContext
} from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, ReferenceLine
} from 'recharts';
import {
  Activity, Zap, AlertTriangle, TrendingUp, TrendingDown, BarChart2, Settings, X, 
  RefreshCw, Cpu, ToggleLeft, ToggleRight, ArrowUp, ArrowDown, Minus, Upload, 
  Database, ChevronDown, Flame, Sun, Moon, Eye
} from 'lucide-react';

// ── Constants & Configuration ──────────────────────────────────────────────────
const API_BASE         = 'http://localhost:8000';
const WS_URL           = 'ws://localhost:8000/ws';

// ── Theming System ─────────────────────────────────────────────────────────────
const themes = {
  dark: {
    name: 'dark',
    bg: '#0a0a0c', surface: '#121216', surfaceHigh: '#1c1c21',
    border: '#27272f', borderHigh: '#3f3f4a',
    text: '#f3f4f6', textMuted: '#9ca3af', textSub: '#6b7280',
    green: '#10b981', greenDim: 'rgba(16, 185, 129, 0.15)',
    red: '#ef4444', redDim: 'rgba(239, 68, 68, 0.15)',
    blue: '#3b82f6', amber: '#f59e0b', amberDim: 'rgba(245, 158, 11, 0.15)',
    purple: '#8b5cf6', teal: '#14b8a6', orange: '#f97316', cyan: '#06b6d4',
    shadow: '0 4px 24px -4px rgba(0, 0, 0, 0.5)'
  },
  light: {
    name: 'light',
    bg: '#f3f4f6', surface: '#ffffff', surfaceHigh: '#f9fafb',
    border: '#e5e7eb', borderHigh: '#d1d5db',
    text: '#111827', textMuted: '#6b7280', textSub: '#9ca3af',
    green: '#059669', greenDim: 'rgba(5, 150, 105, 0.1)',
    red: '#dc2626', redDim: 'rgba(220, 38, 38, 0.1)',
    blue: '#2563eb', amber: '#d97706', amberDim: 'rgba(217, 119, 6, 0.1)',
    purple: '#7c3aed', teal: '#0d9488', orange: '#ea580c', cyan: '#0891b2',
    shadow: '0 4px 20px -2px rgba(0, 0, 0, 0.05)'
  }
};

const ThemeContext = createContext();
const useTheme = () => useContext(ThemeContext);

function ThemeProvider({ children }) {
  const [themeName, setThemeName] = useState('dark');
  const theme = themes[themeName];
  const toggleTheme = () => setThemeName(prev => prev === 'dark' ? 'light' : 'dark');
  
  return (
    <ThemeContext.Provider value={{ theme, toggleTheme }}>
      {children}
    </ThemeContext.Provider>
  );
}

// ── Global Styles ──────────────────────────────────────────────────────────────
function GlobalStyles() {
  const { theme } = useTheme();
  return (
    <style>{`
      * { box-sizing: border-box; }
      body { 
        margin: 0; 
        font-family: 'Inter', system-ui, -apple-system, sans-serif; 
        background-color: ${theme.bg}; 
        color: ${theme.text};
        -webkit-font-smoothing: antialiased;
      }
      ::-webkit-scrollbar { width: 6px; height: 6px; }
      ::-webkit-scrollbar-track { background: transparent; }
      ::-webkit-scrollbar-thumb { background: ${theme.border}; border-radius: 4px; }
      ::-webkit-scrollbar-thumb:hover { background: ${theme.borderHigh}; }
      
      .btn-hover { transition: all 0.2s ease; }
      .btn-hover:hover { background-color: ${theme.surfaceHigh}; border-color: ${theme.borderHigh}; }
      
      .nav-tab { transition: all 0.2s ease; border-bottom: 2px solid transparent; }
      .nav-tab:hover:not(.active) { color: ${theme.text} !important; }
      
      .row-hover { transition: background-color 0.15s ease; cursor: pointer; }
      .row-hover:hover { background-color: ${theme.surfaceHigh} !important; }
      @keyframes benchBarIn {
        from { transform: scaleX(0); opacity: 0.45; }
        to { transform: scaleX(1); opacity: 1; }
      }
    `}</style>
  );
}

// ── Micro-helpers ──────────────────────────────────────────────────────────────
const fmt = (n, d = 2) => (+(n ?? 0)).toFixed(d);
const fmtUSD = (n) => `$${(+(n ?? 0)).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

// ── Reusable Primitives ────────────────────────────────────────────────────────
function SignalBadge({ signal }) {
  const { theme } = useTheme();
  const map = {
    BUY:  { color: theme.green, bg: theme.greenDim, icon: <ArrowUp size={12}/> },
    SELL: { color: theme.red,   bg: theme.redDim,   icon: <ArrowDown size={12}/> },
    HOLD: { color: theme.amber, bg: theme.amberDim, icon: <Minus size={12}/> },
  };
  const s = map[signal] ?? { color: theme.textMuted, bg: theme.surface, icon: null };
  return (
    <span style={{ 
      display: 'inline-flex', alignItems: 'center', gap: 4, 
      padding: '4px 10px', borderRadius: 6,
      backgroundColor: s.bg, color: s.color, 
      fontWeight: 600, fontSize: 11, letterSpacing: '0.05em' 
    }}>
      {s.icon}{signal ?? '—'}
    </span>
  );
}

function Card({ children, style = {} }) {
  const { theme } = useTheme();
  return (
    <div style={{ 
      backgroundColor: theme.surface, 
      border: `1px solid ${theme.border}`,
      borderRadius: 12, 
      padding: 20, 
      boxShadow: theme.shadow,
      display: 'flex', flexDirection: 'column',
      ...style 
    }}>
      {children}
    </div>
  );
}

function StatRow({ label, value, valueColor }) {
  const { theme } = useTheme();
  return (
    <div style={{ 
      display: 'flex', justifyContent: 'space-between', alignItems: 'center',
      gap: 12, padding: '8px 0', borderBottom: `1px solid ${theme.border}` 
    }}>
      <span style={{ color: theme.textMuted, fontSize: 13, fontWeight: 500, minWidth: 0 }}>{label}</span>
      <span style={{ color: valueColor ?? theme.text, fontWeight: 600, fontSize: 14, minWidth: 0, textAlign: 'right', overflowWrap: 'anywhere' }}>{value}</span>
    </div>
  );
}

function BenchmarkComparison({ data, serialKey, parallelKey, unit, color, serialLabel, parallelLabel }) {
  const { theme } = useTheme();
  const serial = Number(data?.[serialKey] ?? 0);
  const parallel = Number(data?.[parallelKey] ?? 0);
  const ready = serial > 0 && parallel > 0;
  const serialWidth = ready ? 100 : 0;
  const parallelWidth = ready ? Math.max(5, Math.min(100, (parallel / serial) * 100)) : 0;
  const runtime = (value) => ready ? `${fmt(value, unit === 'ms' ? 1 : 2)}${unit}` : '-';

  return (
    <div style={{ borderLeft: `3px solid ${color}`, paddingLeft: 12, marginBottom: 16 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 10 }}>
        <span style={{ color: theme.textSub, fontSize: 11, fontWeight: 700, textTransform: 'uppercase' }}>Runtime Comparison</span>
        <span style={{ color, fontSize: 12, fontWeight: 700 }}>{ready ? `${fmt(serial / parallel, 2)}x faster` : 'Awaiting run'}</span>
      </div>

      {[
        { label: serialLabel, value: serial, width: serialWidth, barColor: theme.textMuted },
        { label: parallelLabel, value: parallel, width: parallelWidth, barColor: color },
      ].map(item => (
        <div key={item.label} style={{ marginBottom: 8 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, fontSize: 11, marginBottom: 4 }}>
            <span style={{ color: item.barColor, fontWeight: 700 }}>{item.label}</span>
            <span style={{ color: theme.text, fontWeight: 600 }}>{runtime(item.value)}</span>
          </div>
          <div style={{ height: 8, backgroundColor: theme.bg, border: `1px solid ${theme.border}`, borderRadius: 6, overflow: 'hidden' }}>
            <div style={{
              width: `${item.width}%`,
              height: '100%',
              backgroundColor: item.barColor,
              transformOrigin: 'left',
              animation: ready ? 'benchBarIn 700ms ease-out both' : 'none',
            }} />
          </div>
        </div>
      ))}
    </div>
  );
}

function Pill({ label, color, icon: Icon }) {
  const { theme } = useTheme();
  const baseColor = color ?? theme.textMuted;
  return (
    <span style={{ 
      display: 'inline-flex', alignItems: 'center', gap: 6,
      fontSize: 11, fontWeight: 600, letterSpacing: '0.05em', 
      padding: '6px 12px', borderRadius: 8, color: baseColor, 
      border: `1px solid ${baseColor}33`, 
      backgroundColor: `${baseColor}11` 
    }}>
      {Icon && <Icon size={12} />}
      {label}
    </span>
  );
}

// ── 1. Startup Flow ────────────────────────────────────────────────────────────
function StartupWizard({ onComplete }) {
  const { theme } = useTheme();
  const [step, setStep] = useState(1);
  const fileInputRef = useRef(null);
  
  const setupSystem = async (payload) => {
    onComplete();
    fetch(`${API_BASE}/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }).catch(console.error);
  };

  const handleFileUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const form = new FormData(); form.append('file', file);
    const res = await fetch(`${API_BASE}/upload`, { method: 'POST', body: form });
    if (res.ok) {
      const data = await res.json();
      setupSystem({ data_mode: 'replay', replay_filepath: data.path });
    }
  };

  return (
    <div style={{ position: 'fixed', inset: 0, zIndex: 9999, backgroundColor: theme.bg, display: 'flex', alignItems: 'center', justifyContent: 'center', color: theme.text }}>
      <div style={{ width: 540, backgroundColor: theme.surface, padding: 40, borderRadius: 16, border: `1px solid ${theme.border}`, boxShadow: theme.shadow, textAlign: 'center' }}>
        <Activity size={56} color={theme.cyan} style={{ marginBottom: 20 }}/>
        <h1 style={{ margin: '0 0 12px 0', fontSize: 26, fontWeight: 700, letterSpacing: '-0.02em' }}>HFT System Initialization</h1>
        <p style={{ color: theme.textMuted, fontSize: 15, marginBottom: 40 }}>Select your data ingestion pipeline to begin.</p>

        {step === 1 && (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
            <button className="btn-hover" onClick={() => setupSystem({ data_mode: 'synthetic' })} style={{ padding: 28, borderRadius: 12, backgroundColor: theme.surfaceHigh, border: `1px solid ${theme.border}`, color: theme.text, cursor: 'pointer', display:'flex', flexDirection:'column', alignItems:'center' }}>
              <Database size={36} color={theme.green} style={{ marginBottom: 16 }}/>
              <div style={{ fontWeight: 600, fontSize: 16 }}>Live Simulation</div>
              <div style={{ fontSize: 13, color: theme.textMuted, marginTop: 8 }}>Synthetic tick data via stochastic models.</div>
            </button>
            <button className="btn-hover" onClick={() => setStep(2)} style={{ padding: 28, borderRadius: 12, backgroundColor: theme.surfaceHigh, border: `1px solid ${theme.border}`, color: theme.text, cursor: 'pointer', display:'flex', flexDirection:'column', alignItems:'center' }}>
              <Upload size={36} color={theme.blue} style={{ marginBottom: 16 }}/>
              <div style={{ fontWeight: 600, fontSize: 16 }}>Historical Replay</div>
              <div style={{ fontSize: 13, color: theme.textMuted, marginTop: 8 }}>Stream a structured CSV dataset.</div>
            </button>
          </div>
        )}

        {step === 2 && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <button className="btn-hover" onClick={() => setupSystem({ data_mode: 'replay', replay_filepath: 'data/raw/SP500_Historical_Data.csv' })} style={{ padding: 20, borderRadius: 12, backgroundColor: theme.surfaceHigh, border: `1px solid ${theme.border}`, color: theme.text, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 16 }}>
              <Database color={theme.blue} size={24}/>
              <div style={{ textAlign: 'left' }}>
                <div style={{ fontWeight: 600, fontSize: 15 }}>Use Default Dataset</div>
                <div style={{ fontSize: 13, color: theme.textMuted, marginTop: 4 }}>S&P 500 Historical Data</div>
              </div>
            </button>
            <div className="btn-hover" style={{ padding: 20, borderRadius: 12, border: `2px dashed ${theme.borderHigh}`, cursor: 'pointer', backgroundColor: theme.bg }} onClick={() => fileInputRef.current?.click()}>
              <Upload color={theme.textMuted} size={24} style={{ marginBottom: 12 }}/>
              <div style={{ fontWeight: 600, fontSize: 15 }}>Upload Custom CSV</div>
              <div style={{ fontSize: 13, color: theme.textMuted, marginTop: 4 }}>Format: Timestamp, Symbol, Open, High, Low, Close, Volume</div>
            </div>
            <input ref={fileInputRef} type="file" accept=".csv" style={{ display: 'none' }} onChange={handleFileUpload} />
            <button onClick={() => setStep(1)} style={{ marginTop: 24, background: 'none', border: 'none', color: theme.textSub, cursor: 'pointer', fontWeight: 500, fontSize: 14 }}>← Back to selection</button>
          </div>
        )}
      </div>
    </div>
  );
}

// ── 2. Unified Settings Modal ──────────────────────────────────────────────────
function UnifiedConfigModal({ open, onClose, config, onConfigChange, onResetPortfolio }) {
  const { theme } = useTheme();
  const [local, setLocal] = useState({ ...config });
  const [nerdMode, setNerdMode] = useState(false);

  useEffect(() => { if (open) setLocal({ ...config }); }, [open, config]);

  const apply = async (updates) => {
    const next = { ...local, ...updates };
    setLocal(next);
    await fetch(`${API_BASE}/config`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(updates) });
    onConfigChange(next);
  };

  if (!open) return null;
  return (
    <div style={{ position: 'fixed', inset: 0, zIndex: 1000, display: 'flex', alignItems: 'center', justifyContent: 'center', backgroundColor: 'rgba(0,0,0,0.6)', backdropFilter: 'blur(4px)' }}>
      <div style={{ width: 500, backgroundColor: theme.surface, padding: 32, borderRadius: 16, border: `1px solid ${theme.border}`, boxShadow: theme.shadow, maxHeight: '90vh', overflowY: 'auto' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 28 }}>
          <h2 style={{ margin: 0, fontSize: 20, fontWeight: 600 }}>System Configuration</h2>
          <button onClick={onClose} className="btn-hover" style={{ background: 'none', border: 'none', color: theme.textMuted, cursor: 'pointer', padding: 4, borderRadius: 4 }}><X size={20}/></button>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 13, color: theme.textMuted, marginBottom: 12, fontWeight: 500 }}>
              <span>Tick Speed (Interval)</span><span style={{ color: theme.cyan, fontWeight: 700 }}>{local.tick_interval_seconds ?? 2}s</span>
            </div>
            <input type="range" min={0.1} max={5} step={0.1} value={local.tick_interval_seconds ?? 2} onChange={e => apply({ tick_interval_seconds: Number(e.target.value) })} style={{ width: '100%', accentColor: theme.cyan }}/>
          </div>

          <div>
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              fontSize: 13,
              color: theme.textMuted,
              marginBottom: 12,
              fontWeight: 500
            }}>
              <span>Live Market Workload (Symbols/Tick)</span>
              <span style={{ color: theme.red, fontWeight: 700 }}>
                {local.stream_workload ?? 100}
              </span>
            </div>
            <input
              type="range"
              min={10}
              max={500}
              step={10}
              value={local.stream_workload ?? 100}
              onChange={e => apply({ stream_workload: Number(e.target.value) })}
              style={{ width: '100%', accentColor: theme.red }}
            />
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              fontSize: 11,
              color: theme.textSub,
              marginTop: 4
            }}>
              <span>10 symbols (light)</span>
              <span>500 symbols (stress)</span>
            </div>
          </div>

          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '16px', backgroundColor: theme.surfaceHigh, borderRadius: 10, border: `1px solid ${theme.border}` }}>
            <div>
              <div style={{ fontWeight: 600, fontSize: 14 }}>Auto Trading</div>
              <div style={{ fontSize: 12, color: theme.textMuted, marginTop: 4 }}>Execute trades automatically</div>
            </div>
            <button onClick={() => apply({ auto_trade: !local.auto_trade })} style={{ background: 'none', border: 'none', cursor: 'pointer', color: local.auto_trade ? theme.green : theme.textMuted }}>
              {local.auto_trade ? <ToggleRight size={36}/> : <ToggleLeft size={36}/>}
            </button>
          </div>

          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 13, color: theme.textMuted, marginBottom: 12, fontWeight: 500 }}>
              <span>Benchmark Workload</span><span style={{ color: theme.cyan, fontWeight: 700 }}>{local.benchmark_workload ?? 5000}</span>
            </div>
            <input type="range" min={1000} max={10000} step={1000} value={local.benchmark_workload ?? 5000} onChange={e => apply({ benchmark_workload: Number(e.target.value) })} style={{ width: '100%', accentColor: theme.blue }}/>
          </div>
        </div>

        <div style={{ marginTop: 28, paddingTop: 24, borderTop: `1px solid ${theme.border}` }}>
          <button onClick={() => setNerdMode(!nerdMode)} style={{ background: 'none', border: 'none', color: theme.textSub, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8, fontSize: 13, fontWeight: 600, padding: 0 }}>
            {nerdMode ? <ChevronDown size={16}/> : <ChevronDown size={16} style={{ transform: 'rotate(-90deg)' }}/>}
            Advanced Settings (Nerd Mode)
          </button>
          
          {nerdMode && (
            <div style={{ marginTop: 16, display: 'flex', flexDirection: 'column', gap: 16, padding: 20, backgroundColor: theme.bg, borderRadius: 10, border: `1px solid ${theme.border}` }}>
              <div>
                <div style={{ fontSize: 12, color: theme.textMuted, marginBottom: 8, fontWeight: 500 }}>SMA Window</div>
                <input type="number" value={local.sma_window ?? 20} onChange={e => apply({ sma_window: Number(e.target.value) })} style={{ width: '100%', padding: '10px 12px', backgroundColor: theme.surface, color: theme.text, border: `1px solid ${theme.borderHigh}`, borderRadius: 6, outline: 'none' }}/>
              </div>
              <div>
                <div style={{ fontSize: 12, color: theme.textMuted, marginBottom: 8, fontWeight: 500 }}>RSI Window</div>
                <input type="number" value={local.rsi_window ?? 14} onChange={e => apply({ rsi_window: Number(e.target.value) })} style={{ width: '100%', padding: '10px 12px', backgroundColor: theme.surface, color: theme.text, border: `1px solid ${theme.borderHigh}`, borderRadius: 6, outline: 'none' }}/>
              </div>
              <button onClick={() => { onResetPortfolio(); onClose(); }} className="btn-hover" style={{ marginTop: 8, width: '100%', padding: 12, borderRadius: 8, backgroundColor: theme.redDim, color: theme.red, fontWeight: 600, border: `1px solid ${theme.red}`, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8 }}>
                <RefreshCw size={16}/> Reset Paper Portfolio
              </button>
            </div>
          )}
        </div>

        <button onClick={onClose} style={{ width: '100%', marginTop: 28, padding: '14px', borderRadius: 8, backgroundColor: theme.blue, color: '#fff', fontWeight: 600, fontSize: 15, border: 'none', cursor: 'pointer', transition: 'opacity 0.2s' }} onMouseEnter={e=>e.target.style.opacity=0.9} onMouseLeave={e=>e.target.style.opacity=1}>
          Done
        </button>
      </div>
    </div>
  );
}

// ── 3. Main Dashboard UI ───────────────────────────────────────────────────────
function MainDashboard() {
  const { theme, toggleTheme } = useTheme();
  const wsRef             = useRef(null);
  const reconnectRef      = useRef(null);
  const reconnectAttempts = useRef(0);
  const pendingTickRef    = useRef(null);
  const renderTimerRef    = useRef(null);

  const [globalState, setGlobalState]         = useState(null);
  const [liveConfig, setLiveConfig]           = useState({
    tick_interval_seconds: 2.0,
    stream_workload:       100,
  });
  const [allTickers, setAllTickers]           = useState([]);
  const [watchlist, setWatchlist]             = useState(["AAPL","TSLA","NVDA"]);
  const [selectedStock, setSelectedStock]     = useState("AAPL");
  const [wsStatus, setWsStatus]               = useState('connecting');
  const [trades, setTrades]                   = useState([]);
  const [activeTab, setActiveTab]             = useState('chart');
  const [sysConfigOpen, setSysConfigOpen]     = useState(false);
  const [benchMode, setBenchMode]             = useState("parallel");
  const [benchML, setBenchML]             = useState(false);
  const [benchStream, setBenchStream]     = useState(false);
  const [optStream, setOptStream] = useState(false);
  const [benchCompute, setBenchCompute]   = useState(false);
  const [hasStarted, setHasStarted]           = useState(false);

  // Chart toggles
  const [showSMA, setShowSMA]       = useState(true);
  const [showRSI, setShowRSI]       = useState(true);
  const [showMACD, setShowMACD]     = useState(true);
  const [showTarget, setShowTarget] = useState(true);

  const connectWS = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;
    setWsStatus('connecting');
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen  = () => { setWsStatus('connected'); reconnectAttempts.current = 0; };
    ws.onerror = () => setWsStatus('error');
    ws.onclose = () => {
      setWsStatus('error');
      const delay = Math.min(1000 * 2 ** reconnectAttempts.current, 15_000);
      reconnectAttempts.current++;
      reconnectRef.current = setTimeout(connectWS, delay);
    };
    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'state_update') {
          setGlobalState(msg.data);
          if (msg.data.config) setLiveConfig(msg.data.config);
        } else if (msg.type === 'tick_update') {
          const prevPending = pendingTickRef.current;
          pendingTickRef.current = {
            ...msg.data,
            symbols: {
              ...(prevPending?.symbols ?? {}),
              ...(msg.data.symbols ?? {}),
            },
          };
          if (!renderTimerRef.current) {
            renderTimerRef.current = setTimeout(() => {
              const delta = pendingTickRef.current;
              pendingTickRef.current = null;
              renderTimerRef.current = null;
              if (!delta) return;
              setGlobalState(prev => {
                if (!prev) return prev;
                const next = {
                  ...prev,
                  global_trading: delta.global_trading ?? prev.global_trading,
                  top_movers: delta.top_movers ?? prev.top_movers,
                  config: delta.config ?? prev.config,
                };
                if (delta.replay_accuracy) next.replay_accuracy = delta.replay_accuracy;
                Object.entries(delta.symbols ?? {}).forEach(([sym, patch]) => {
                  const oldState = next[sym] ?? { market: { history: [] }, indicators: {}, signals: {}, metrics: {}, anomalies: [] };
                  const oldHistory = oldState.market?.history ?? [];
                  const point = patch.market?.history_point;
                  const history = point ? [...oldHistory.slice(-99), point] : oldHistory;
                  next[sym] = {
                    ...oldState,
                    market: {
                      ...(oldState.market ?? {}),
                      ...(patch.market ?? {}),
                      history,
                    },
                    indicators: patch.indicators ?? oldState.indicators,
                    signals: patch.signals ?? oldState.signals,
                    metrics: patch.metrics ?? oldState.metrics,
                    anomalies: patch.anomalies ?? oldState.anomalies,
                  };
                  delete next[sym].market.history_point;
                });
                return next;
              });
              if (delta.config) setLiveConfig(delta.config);
            }, 200);
          }
        } else if (msg.type === 'config_update') {
          setLiveConfig(msg.data);
        }
      } catch (_) {}
    };
  }, []);

  useEffect(() => {
    fetch(`${API_BASE}/state`).then(r => r.json()).then(d => {
      setGlobalState(d);
      if (d.config) setLiveConfig(d.config);
    }).catch(() => {});
    fetch(`${API_BASE}/config`).then(r => r.json()).then(setLiveConfig).catch(() => {});
    
    // NEW: Fetch all available symbols for the dropdown
    fetch(`${API_BASE}/symbols`)
      .then(r => r.json())
      .then(d => {
        console.log("📡 Fetched symbols:", d);
        setAllTickers(d.symbols || []);
      })
      .catch(e => console.error("Failed to fetch symbols:", e));
      
    connectWS();
    return () => {
      clearTimeout(reconnectRef.current);
      clearTimeout(renderTimerRef.current);
      wsRef.current?.close();
    };
  }, [connectWS]);

  useEffect(() => {
    if (!selectedStock) return;
    fetch(`${API_BASE}/trades?symbol=${selectedStock}&limit=50`)
      .then(r => r.json()).then(setTrades).catch(() => {});
  }, [selectedStock]);

  useEffect(() => {
    if (selectedStock && !watchlist.includes(selectedStock)) {
      setSelectedStock(watchlist[0] ?? null);
    }
  }, [watchlist, selectedStock]);

  // THE PUB/SUB SUBSCRIPTION MECHANISM
  useEffect(() => {
    if (watchlist.length === 0) return;
    const payload = { 
      active_symbols: watchlist.map(sym => ({ symbol: sym, base_price: 100.0 })) 
    };
    console.log("📨 Pub/Sub: Posting watchlist to /config:", payload);
    fetch(`${API_BASE}/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }).catch(e => console.error("Watchlist POST failed:", e));
  }, [watchlist]);

  const handleResetPortfolio = async () => {
    try { await fetch(`${API_BASE}/reset-portfolio`, { method: 'POST' }); } catch (e) { console.error(e); }
  };

  const triggerSpecificBenchmark = async (type, setLoadState, isOpt = false) => {
    setLoadState(true);
    const workload = liveConfig.benchmark_workload ?? 5000;
    try {
      await fetch(`${API_BASE}/benchmark`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type, mode: benchMode, workload, optimized: isOpt }) // <--- added here
      });
    } catch (e) { console.error(e); }
    setLoadState(false);
  };

  const tickFormatter = useCallback((time) => {
    try { return new Date(time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }); }
    catch { return ''; }
  }, []);

  const portfolio  = globalState?.global_trading;
  const stockState = selectedStock ? globalState?.[selectedStock] : null;
  const benchmark  = globalState?.benchmark ?? {};
  const benchmarkSuite = globalState?.benchmark_suite;
  const mlData = benchmarkSuite?.big_data_ml;
  const streamData = benchmarkSuite?.streaming_latency;
  const mcData = benchmarkSuite?.monte_carlo;
  const topMovers  = globalState?.top_movers ?? [];

  const pnl = useMemo(() => {
    if (!portfolio) return 0;
    return (portfolio.portfolio_value ?? 0) - (portfolio.starting_capital ?? 100_000);
  }, [portfolio]);

  const rsiColor = (rsi) => rsi > 70 ? theme.red : rsi < 30 ? theme.green : theme.amber;
  const autoTradeOn = liveConfig.auto_trade === true;

  const marketConditionMap = {
    normal:   { label: 'NORMAL',   icon: Activity,      color: theme.blue },
    bull:     { label: 'BULL',     icon: TrendingUp,    color: theme.green },
    bear:     { label: 'BEAR',     icon: TrendingDown,  color: theme.red },
    crash:    { label: 'CRASH',    icon: AlertTriangle, color: theme.red },
    volatile: { label: 'VOLATILE', icon: Zap,           color: theme.amber },
  };

  if (!hasStarted) return <StartupWizard onComplete={() => setHasStarted(true)} />;

  if (!globalState) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100vh', backgroundColor: theme.bg, color: theme.textSub, flexDirection: 'column', gap: 16 }}>
        <Activity size={40} style={{ opacity: 0.5, animation: 'pulse 2s infinite' }} color={theme.cyan}/>
        <span style={{ fontSize: 15, fontWeight: 500, letterSpacing: '0.05em' }}>Connecting to HFT Cluster...</span>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', height: '100vh', backgroundColor: theme.bg, color: theme.text, overflow: 'hidden' }}>
      
      {/* ── SIDEBAR ────────────────────────────────────────────────────────── */}
      <div style={{ width: 260, backgroundColor: theme.surface, borderRight: `1px solid ${theme.border}`, display: 'flex', flexDirection: 'column', flexShrink: 0, zIndex: 10 }}>
        
        {/* Header */}
        <div style={{ padding: '20px 20px 16px', borderBottom: `1px solid ${theme.border}` }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <div style={{ backgroundColor: theme.surfaceHigh, padding: 6, borderRadius: 8, border: `1px solid ${theme.borderHigh}` }}>
              <Activity size={18} color={theme.cyan}/>
            </div>
            <span style={{ fontWeight: 700, fontSize: 14, letterSpacing: '0.08em' }}>HFT TERMINAL</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 12 }}>
            <div style={{ width: 8, height: 8, borderRadius: '50%', backgroundColor: wsStatus === 'connected' ? theme.green : wsStatus === 'connecting' ? theme.amber : theme.red }}/>
            <span style={{ fontSize: 11, fontWeight: 600, color: theme.textMuted, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              {wsStatus === 'connected' ? 'System Live' : wsStatus === 'connecting' ? 'Reconnecting...' : 'Offline'}
            </span>
            {liveConfig.tick_interval_seconds && (
              <span style={{ marginLeft: 'auto', fontSize: 11, fontWeight: 600, color: theme.cyan }}>
                {(liveConfig.tick_interval_seconds * 1000).toFixed(0)}ms
              </span>
            )}
          </div>
        </div>

        {/* Watchlist */}
        <div style={{ padding: '20px 16px 0' }}>
          <div style={{ fontSize: 11, color: theme.textSub, fontWeight: 600, letterSpacing: '0.08em', marginBottom: 12, textTransform: 'uppercase', paddingLeft: 4 }}>Watchlist</div>
          <select value=""
            onChange={e => {
              const s = e.target.value;
              if (s && !watchlist.includes(s)) { setWatchlist(prev => [...prev, s]); setSelectedStock(s); }
            }}
            style={{ width: '100%', padding: '10px 12px', backgroundColor: theme.bg, color: theme.text, border: `1px solid ${theme.border}`, borderRadius: 8, fontSize: 13, cursor: 'pointer', marginBottom: 12, outline: 'none' }}>
            <option value="" disabled>+ Add ticker...</option>
            {allTickers.filter(s => !watchlist.includes(s)).map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>

        <div style={{ overflowY: 'auto', padding: '0 12px', maxHeight: '35vh' }}>
          {watchlist.map(ticker => {
            const ts = globalState[ticker];
            if (!ts) return null;
            const isUp = (ts.indicators?.momentum ?? 0) >= 0;
            const sig = ts.signals?.current ?? 'HOLD';
            const sigCol = sig === 'BUY' ? theme.green : sig === 'SELL' ? theme.red : theme.amber;
            const isActive = selectedStock === ticker;
            
            return (
              <div key={ticker} className="row-hover" onClick={() => setSelectedStock(ticker)}
                style={{ padding: '12px', borderRadius: 10, cursor: 'pointer', marginBottom: 6,
                  backgroundColor: isActive ? theme.surfaceHigh : 'transparent',
                  border: `1px solid ${isActive ? theme.borderHigh : 'transparent'}`,
                  transition: 'all 0.15s' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span style={{ fontWeight: 600, fontSize: 14 }}>{ticker}</span>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <span style={{ fontSize: 11, color: sigCol, fontWeight: 700 }}>{sig}</span>
                    <button onClick={e => { e.stopPropagation(); setWatchlist(w => w.filter(s => s !== ticker)); }}
                      style={{ background: 'none', border: 'none', cursor: 'pointer', color: theme.textMuted, fontSize: 16, lineHeight: 1, padding: 0 }}>×</button>
                  </div>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 6 }}>
                  <span style={{ fontSize: 13, color: theme.textSub, fontWeight: 500 }}>${fmt(ts.market?.price)}</span>
                  <span style={{ fontSize: 12, color: isUp ? theme.green : theme.red, fontWeight: 600 }}>
                    {isUp ? '▲' : '▼'} {Math.abs(ts.indicators?.momentum ?? 0).toFixed(2)}%
                  </span>
                </div>
              </div>
            );
          })}
        </div>

        {/* Top Movers */}
        {topMovers.length > 0 && (
          <div style={{ padding: '20px 16px 0', borderTop: `1px solid ${theme.border}`, marginTop: 'auto' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12, paddingLeft: 4 }}>
              <Flame size={14} color={theme.orange}/>
              <span style={{ fontSize: 11, fontWeight: 600, color: theme.textSub, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Top Movers</span>
            </div>
            {topMovers.map(mover => (
              <div key={mover.symbol} className="row-hover"
                onClick={() => {
                  if (!watchlist.includes(mover.symbol)) setWatchlist(prev => [...prev, mover.symbol]);
                  setSelectedStock(mover.symbol);
                }}
                style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 12px', borderRadius: 8, marginBottom: 6, border: `1px solid ${theme.border}`, backgroundColor: theme.bg }}>
                <span style={{ fontSize: 13, fontWeight: 600, color: theme.text }}>{mover.symbol}</span>
                <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
                  <span style={{ fontSize: 12, color: theme.textSub }}>${fmt(mover.price)}</span>
                  <span style={{ fontSize: 12, fontWeight: 600, color: mover.momentum >= 0 ? theme.green : theme.red }}>
                    {mover.momentum > 0 ? '+' : ''}{mover.momentum.toFixed(2)}%
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Footer Configuration */}
        <div style={{ padding: '20px', borderTop: `1px solid ${theme.border}`, marginTop: topMovers.length ? 0 : 'auto' }}>
          {liveConfig.market_condition && (
            <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'center' }}>
              <Pill 
                label={marketConditionMap[liveConfig.market_condition]?.label ?? liveConfig.market_condition.toUpperCase()}
                color={marketConditionMap[liveConfig.market_condition]?.color ?? theme.amber}
                icon={marketConditionMap[liveConfig.market_condition]?.icon} 
              />
            </div>
          )}
          <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'center' }}>
            <Pill
              label={autoTradeOn ? 'AUTO TRADE' : 'SUGGESTION MODE'}
              color={autoTradeOn ? theme.green : theme.amber}
              icon={autoTradeOn ? Cpu : Eye} 
            />
          </div>
          
          <div style={{ display: 'flex', gap: 10 }}>
            <button className="btn-hover" onClick={() => setSysConfigOpen(true)}
              style={{ flex: 1, padding: '10px', borderRadius: 8, border: `1px solid ${theme.border}`, backgroundColor: theme.surfaceHigh, color: theme.text, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8, fontSize: 13, fontWeight: 500 }}>
              <Settings size={16}/> System Config
            </button>
            <button className="btn-hover" onClick={toggleTheme}
              style={{ padding: '10px', borderRadius: 8, border: `1px solid ${theme.border}`, backgroundColor: theme.surfaceHigh, color: theme.text, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              {theme.name === 'dark' ? <Sun size={16}/> : <Moon size={16}/>}
            </button>
          </div>
        </div>
      </div>

      {/* ── MAIN CONTENT ────────────────────────────────────────────────────── */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        
        {/* Top bar */}
        <div style={{ backgroundColor: theme.surface, borderBottom: `1px solid ${theme.border}`, padding: '0 24px', display: 'flex', alignItems: 'center', gap: 24, height: 72, flexShrink: 0 }}>
          {stockState ? (
            <>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 16 }}>
                <span style={{ fontWeight: 700, fontSize: 24, letterSpacing: '-0.02em' }}>{selectedStock}</span>
                <span style={{ fontSize: 20, fontWeight: 600, color: theme.text }}>${fmt(stockState.market?.price)}</span>
                <span style={{ fontSize: 14, fontWeight: 600, color: (stockState.indicators?.momentum ?? 0) >= 0 ? theme.green : theme.red }}>
                  {(stockState.indicators?.momentum ?? 0) >= 0 ? '+' : ''}{fmt(stockState.indicators?.momentum)}%
                </span>
              </div>
              
              <div style={{ display: 'flex', gap: 16, alignItems: 'center', borderLeft: `1px solid ${theme.border}`, paddingLeft: 24 }}>
                <SignalBadge signal={stockState.signals?.current ?? 'HOLD'} />
                <span style={{ fontSize: 13, color: theme.textSub, fontWeight: 500 }}>
                  Confidence: <span style={{ color: theme.cyan, fontWeight: 700 }}>{stockState.signals?.confidence ?? 0}%</span>
                </span>
              </div>
            </>
          ) : (
            <span style={{ color: theme.textMuted, fontSize: 15 }}>Select a stock from the watchlist to view details</span>
          )}

          <div style={{ marginLeft: 'auto', display: 'flex', gap: 32, alignItems: 'center' }}>
            {[
              { label: 'Portfolio Value', value: fmtUSD(portfolio?.portfolio_value) },
              { label: 'Total P&L',       value: `${pnl >= 0 ? '+' : ''}${fmtUSD(Math.abs(pnl))}`, color: pnl >= 0 ? theme.green : theme.red },
              { label: 'Available Cash',  value: fmtUSD(portfolio?.cash) },
            ].map(({ label, value, color }) => (
              <div key={label} style={{ textAlign: 'right' }}>
                <div style={{ fontSize: 11, color: theme.textSub, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4, fontWeight: 600 }}>{label}</div>
                <div style={{ fontSize: 16, fontWeight: 700, color: color ?? theme.text }}>{value}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Tab navigation */}
        <div style={{ backgroundColor: theme.surface, borderBottom: `1px solid ${theme.border}`, padding: '0 24px', display: 'flex', gap: 12, flexShrink: 0 }}>
          {[
            { id: 'chart',       label: 'Price Chart',   icon: <BarChart2 size={16}/> },
            { id: 'indicators',  label: 'Indicators',     icon: <Activity size={16}/> },
            { id: 'trades',      label: 'Trade History', icon: <TrendingUp size={16}/> },
            { id: 'benchmark',   label: 'System Perf.',  icon: <Cpu size={16}/> },
          ].map(tab => (
            <button key={tab.id} className={`nav-tab ${activeTab === tab.id ? 'active' : ''}`} onClick={() => setActiveTab(tab.id)}
              style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '16px 8px', background: 'none', border: 'none', cursor: 'pointer', fontSize: 14, fontWeight: 500, color: activeTab === tab.id ? theme.cyan : theme.textMuted }}>
              {tab.icon}{tab.label}
            </button>
          ))}
        </div>

        {/* Tab Content Areas */}
        <div style={{ flex: 1, overflowY: 'auto', padding: 24, display: 'flex', flexDirection: 'column' }}>
          {!stockState ? (
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', color: theme.textMuted, gap: 16 }}>
              <BarChart2 size={48} style={{ opacity: 0.2 }}/>
              <span style={{ fontSize: 15 }}>Awaiting market data...</span>
            </div>
          ) : (
            <>
              {/* ── CHART TAB ─────────────────────────────────────────────── */}
              {activeTab === 'chart' && (
                <div style={{ display: 'grid', gridTemplateColumns: '320px 1fr', gap: 24, height: '100%' }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
                    <Card>
                      <div style={{ fontSize: 11, color: theme.textSub, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 16, fontWeight: 600 }}>Algorithm Analysis</div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20 }}>
                        <SignalBadge signal={stockState.signals?.current ?? 'HOLD'}/>
                        <span style={{ fontSize: 12, color: theme.textMuted, fontWeight: 500 }}>
                          Native LightGBM: <span style={{ color: (stockState.signals?.gbt_direction ?? stockState.signals?.xgb_direction) === 'UP' ? theme.green : theme.red, fontWeight: 700 }}>
                            {stockState.signals?.gbt_direction ?? stockState.signals?.xgb_direction ?? 'FLAT'}
                          </span>
                        </span>
                      </div>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                        <StatRow label="Lasso Target" value={`$${fmt(stockState.signals?.lasso_target)}`}/>
                        <StatRow label="Confidence"   value={`${stockState.signals?.confidence ?? 0}%`} valueColor={theme.cyan}/>
                        <StatRow label="C++ Latency" value={`${fmt(stockState.metrics?.latency_ms, 2)} ms`} valueColor={theme.green}/>
                        <StatRow label="Throughput"   value={`${stockState.metrics?.throughput ?? 0} op/s`}/>
                      </div>
                    </Card>

                    <Card style={{ flex: 1 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
                        <Zap size={14} color={theme.amber}/>
                        <span style={{ fontSize: 11, color: theme.textSub, textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>AI Rationale</span>
                      </div>
                      <div style={{ fontSize: 14, color: theme.text, lineHeight: 1.6, marginBottom: 24 }}>
                        {stockState.signals?.explanation ?? 'Aggregating market state...'}
                      </div>
                      
                      {stockState.anomalies?.length > 0 && (
                        <div style={{ marginTop: 'auto' }}>
                          <div style={{ fontSize: 11, color: theme.textSub, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 12, fontWeight: 600 }}>Anomaly Detection Feed</div>
                          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                            {stockState.anomalies.slice(0, 3).map((a, i) => (
                              <div key={i} style={{ display: 'flex', alignItems: 'flex-start', gap: 10, fontSize: 12, padding: 8, backgroundColor: theme.redDim, borderRadius: 6, border: `1px solid ${theme.border}` }}>
                                <AlertTriangle size={14} color={theme.red} style={{ marginTop: 2 }}/>
                                <div>
                                  <div style={{ color: theme.red, fontWeight: 600, marginBottom: 2 }}>{a.alert}</div>
                                  <div style={{ color: theme.textMuted, fontSize: 11 }}>{a.time}</div>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </Card>

                    {liveConfig?.data_mode === 'replay' && globalState?.replay_accuracy && (
                      <Card>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16 }}>
                          <BarChart2 size={16} color={theme.blue} />
                          <span style={{ fontWeight: 700, fontSize: 14, letterSpacing: '-0.01em' }}>
                            Backtesting Accuracy
                          </span>
                          <span style={{ 
                            marginLeft: 'auto', fontSize: 11, color: theme.textSub, 
                            backgroundColor: theme.surfaceHigh, padding: '2px 8px', borderRadius: 4 
                          }}>
                            REPLAY MODE
                          </span>
                        </div>

                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
                          <div style={{ backgroundColor: theme.surfaceHigh, padding: 12, borderRadius: 8, textAlign: 'center', border: `1px solid ${theme.border}` }}>
                            <div style={{ fontSize: 22, fontWeight: 700, color: theme.amber }}>
                              ${fmt(globalState.replay_accuracy.avg_lasso_mae, 4)}
                            </div>
                            <div style={{ fontSize: 11, color: theme.textSub, marginTop: 4, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                              Lasso MAE
                            </div>
                            <div style={{ fontSize: 10, color: theme.textSub, marginTop: 2 }}>mean absolute error vs actual</div>
                          </div>
                          <div style={{ backgroundColor: theme.surfaceHigh, padding: 12, borderRadius: 8, textAlign: 'center', border: `1px solid ${theme.border}` }}>
                            <div style={{ fontSize: 22, fontWeight: 700, color: (globalState.replay_accuracy.avg_gbt_accuracy ?? globalState.replay_accuracy.avg_xgb_accuracy) >= 55 ? theme.green : theme.red }}>
                              {fmt(globalState.replay_accuracy.avg_gbt_accuracy ?? globalState.replay_accuracy.avg_xgb_accuracy, 1)}%
                            </div>
                            <div style={{ fontSize: 11, color: theme.textSub, marginTop: 4, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                              LightGBM Direction
                            </div>
                            <div style={{ fontSize: 10, color: theme.textSub, marginTop: 2 }}>% correct UP/DOWN calls</div>
                          </div>
                        </div>

                        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                          <StatRow 
                            label="Symbols Scored" 
                            value={globalState.replay_accuracy.symbols_scored ?? '—'} 
                          />
                          <StatRow 
                            label="Baseline (random)" 
                            value="50.0%" 
                            valueColor={theme.textSub}
                          />
                          {(globalState.replay_accuracy.avg_gbt_accuracy ?? globalState.replay_accuracy.avg_xgb_accuracy) > 0 && (
                            <StatRow 
                              label="Edge vs Random" 
                              value={`${fmt((globalState.replay_accuracy.avg_gbt_accuracy ?? globalState.replay_accuracy.avg_xgb_accuracy) - 50, 1)}%`}
                              valueColor={(globalState.replay_accuracy.avg_gbt_accuracy ?? globalState.replay_accuracy.avg_xgb_accuracy) >= 50 ? theme.green : theme.red}
                            />
                          )}
                        </div>
                      </Card>
                    )}
                  </div>

                  <Card style={{ padding: 24 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
                      <span style={{ fontSize: 14, fontWeight: 600, color: theme.text }}>Real-Time Execution Chart</span>
                      <div style={{ display: 'flex', gap: 8 }}>
                        {[
                          { key: 'target', state: showTarget, set: setShowTarget, label: 'Lasso Target', color: theme.amber },
                          { key: 'sma',    state: showSMA,    set: setShowSMA,    label: `SMA ${liveConfig.sma_window ?? 20}`, color: theme.purple },
                          { key: 'rsi',    state: showRSI,    set: setShowRSI,    label: `RSI ${liveConfig.rsi_window ?? 14}`, color: theme.teal },
                          { key: 'macd',   state: showMACD,   set: setShowMACD,   label: 'MACD', color: theme.orange },
                        ].map(({ key, state, set, label, color }) => (
                          <button key={key} className="btn-hover" onClick={() => set(!state)}
                            style={{ padding: '6px 12px', borderRadius: 6, cursor: 'pointer', fontSize: 11, fontWeight: 600, border: `1px solid ${state ? color : theme.border}`, backgroundColor: state ? `${color}15` : 'transparent', color: state ? color : theme.textMuted }}>
                            {label}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div style={{ flex: 1, minHeight: 400 }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={stockState.market?.history ?? []} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                          <CartesianGrid strokeDasharray="4 4" stroke={theme.border} vertical={false}/>
                          <XAxis dataKey="time" stroke={theme.textMuted} tick={{ fontSize: 11, fill: theme.textMuted }} tickFormatter={tickFormatter} minTickGap={60}/>
                          <YAxis yAxisId="left" domain={['auto', 'auto']} stroke={theme.textMuted} tick={{ fontSize: 11, fill: theme.textMuted }} tickFormatter={v => `$${v.toFixed(0)}`} width={60} axisLine={false} tickLine={false}/>
                          <YAxis yAxisId="right" domain={['auto', 'auto']} stroke={theme.textMuted} tick={{ fontSize: 11, fill: theme.textMuted }} orientation="right" width={40} axisLine={false} tickLine={false}/>
                          <Tooltip contentStyle={{ backgroundColor: theme.surfaceHigh, borderColor: theme.border, color: theme.text, fontSize: 12, borderRadius: 8, boxShadow: theme.shadow }} labelFormatter={tickFormatter}/>
                          
                          <Line yAxisId="left" type="monotone" dataKey="price" stroke={theme.green} strokeWidth={2.5} dot={false} name="Price" isAnimationActive={false}/>
                          {showTarget && <Line yAxisId="left" type="monotone" dataKey="target" stroke={theme.amber} strokeWidth={1.5} strokeDasharray="5 5" dot={false} name="Target" isAnimationActive={false}/>}
                          {showSMA    && <Line yAxisId="left" type="monotone" dataKey="sma" stroke={theme.purple} strokeWidth={1.5} dot={false} name="SMA" isAnimationActive={false}/>}
                          {showRSI    && <Line yAxisId="right" type="monotone" dataKey="rsi" stroke={theme.teal} strokeWidth={1.5} dot={false} name="RSI" isAnimationActive={false}/>}
                          {showMACD   && <Line yAxisId="right" type="monotone" dataKey="macd" stroke={theme.orange} strokeWidth={1.5} dot={false} name="MACD" isAnimationActive={false}/>}
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </Card>
                </div>
              )}

              {/* ── INDICATORS TAB ────────────────────────────────────────── */}
              {activeTab === 'indicators' && (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(450px, 1fr))', gap: 24 }}>
                  <Card>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 20, alignItems: 'center' }}>
                      <span style={{ fontSize: 14, fontWeight: 600, color: theme.text }}>Relative Strength Index (RSI)</span>
                      <span style={{ fontSize: 18, fontWeight: 700, color: rsiColor(stockState.indicators?.rsi ?? 50) }}>
                        {fmt(stockState.indicators?.rsi, 1)}
                      </span>
                    </div>
                    <div style={{ height: 200, marginBottom: 12 }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={stockState.market?.history ?? []}>
                          <CartesianGrid strokeDasharray="4 4" stroke={theme.border} vertical={false}/>
                          <YAxis domain={[0, 100]} stroke={theme.textMuted} tick={{ fontSize: 11, fill: theme.textMuted }} width={30} axisLine={false} tickLine={false}/>
                          <Tooltip contentStyle={{ backgroundColor: theme.surfaceHigh, borderColor: theme.border, color: theme.text, fontSize: 12, borderRadius: 8 }} labelFormatter={tickFormatter}/>
                          <ReferenceLine y={70} stroke={theme.red} strokeDasharray="4 4" opacity={0.5}/>
                          <ReferenceLine y={30} stroke={theme.green} strokeDasharray="4 4" opacity={0.5}/>
                          <Line type="monotone" dataKey="rsi" stroke={theme.teal} strokeWidth={2} dot={false} isAnimationActive={false}/>
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: theme.textMuted, fontWeight: 500 }}>
                      <span style={{ color: theme.green }}>&lt; 30 (Oversold Area)</span>
                      <span style={{ color: theme.red }}>&gt; 70 (Overbought Area)</span>
                    </div>
                  </Card>

                  <Card>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 20, alignItems: 'center' }}>
                      <span style={{ fontSize: 14, fontWeight: 600, color: theme.text }}>MACD (12/26/9)</span>
                      <span style={{ fontSize: 18, fontWeight: 700, color: (stockState.indicators?.macd?.histogram ?? 0) > 0 ? theme.green : theme.red }}>
                        {fmt(stockState.indicators?.macd?.histogram, 4)}
                      </span>
                    </div>
                    <div style={{ height: 160, marginBottom: 16 }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={stockState.market?.history ?? []}>
                          <CartesianGrid strokeDasharray="4 4" stroke={theme.border} vertical={false}/>
                          <YAxis domain={['auto', 'auto']} stroke={theme.textMuted} tick={{ fontSize: 11, fill: theme.textMuted }} width={40} axisLine={false} tickLine={false}/>
                          <Tooltip contentStyle={{ backgroundColor: theme.surfaceHigh, borderColor: theme.border, color: theme.text, fontSize: 12, borderRadius: 8 }} labelFormatter={tickFormatter}/>
                          <ReferenceLine y={0} stroke={theme.textSub} opacity={0.5}/>
                          <Line type="monotone" dataKey="macd" stroke={theme.orange} strokeWidth={2} dot={false} name="MACD" isAnimationActive={false}/>
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                      <StatRow label="MACD Line" value={fmt(stockState.indicators?.macd?.line, 4)}/>
                      <StatRow label="Signal Line" value={fmt(stockState.indicators?.macd?.signal, 4)}/>
                    </div>
                  </Card>

                  <Card>
                    <div style={{ fontSize: 14, fontWeight: 600, color: theme.text, marginBottom: 16 }}>Market Summary</div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                      <StatRow label="Current Price" value={`$${fmt(stockState.market?.price)}`}/>
                      <StatRow label={`SMA (${liveConfig.sma_window ?? 20})`} value={`$${fmt(stockState.indicators?.sma_20)}`}/>
                      <StatRow label="Momentum" value={`${fmt(stockState.indicators?.momentum)}%`} valueColor={(stockState.indicators?.momentum ?? 0) >= 0 ? theme.green : theme.red}/>
                      <StatRow label="RSI" value={fmt(stockState.indicators?.rsi, 1)} valueColor={rsiColor(stockState.indicators?.rsi ?? 50)}/>
                      <StatRow label="Tick Latency" value={`${fmt(stockState.metrics?.latency_ms, 1)} ms`} valueColor={stockState.metrics?.latency_ms > (liveConfig.tick_interval_seconds * 1000) ? theme.red : theme.cyan}/>
                    </div>
                  </Card>

                  <Card>
                    <div style={{ fontSize: 14, fontWeight: 600, color: theme.text, marginBottom: 16 }}>Active Positions</div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                      {Object.entries(portfolio?.positions ?? {}).filter(([, v]) => v > 0).map(([sym, shares]) => (
                        <StatRow key={sym} label={sym} value={`${shares} shares`} valueColor={theme.cyan}/>
                      ))}
                      {Object.values(portfolio?.positions ?? {}).every(v => v === 0) && (
                        <span style={{ fontSize: 13, color: theme.textMuted, padding: '12px 0' }}>No open positions.</span>
                      )}
                    </div>
                    <div style={{ marginTop: 'auto', paddingTop: 16, borderTop: `1px solid ${theme.border}` }}>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                        <StatRow label="Available Cash" value={fmtUSD(portfolio?.cash)}/>
                        <StatRow label="Total Portfolio Value" value={fmtUSD(portfolio?.portfolio_value)}/>
                        <StatRow label="Unrealized P&L" value={`${pnl >= 0 ? '+' : ''}${fmtUSD(Math.abs(pnl))}`} valueColor={pnl >= 0 ? theme.green : theme.red}/>
                      </div>
                    </div>
                  </Card>
                </div>
              )}

              {/* ── TRADES TAB ────────────────────────────────────────────── */}
              {activeTab === 'trades' && (
                <Card style={{ flex: 1 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
                    <span style={{ fontSize: 15, fontWeight: 600, color: theme.text }}>Execution Log — {selectedStock}</span>
                    {!autoTradeOn && (
                      <span style={{ fontSize: 11, color: theme.amber, backgroundColor: theme.amberDim, border: `1px solid ${theme.amber}44`, padding: '4px 10px', borderRadius: 6, fontWeight: 600 }}>
                        <Eye size={12} style={{ display: 'inline', marginRight: 4, verticalAlign: 'text-top' }}/>
                        Suggestion Mode Active
                      </span>
                    )}
                  </div>
                  
                  {trades.length === 0 ? (
                    <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: theme.textMuted, fontSize: 14 }}>
                      {autoTradeOn ? `No trades recorded for ${selectedStock} yet.` : 'System is in suggestion mode. Auto-trading is disabled.'}
                    </div>
                  ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 100px 80px', fontSize: 11, color: theme.textSub, textTransform: 'uppercase', letterSpacing: '0.05em', fontWeight: 600, padding: '0 16px 12px', borderBottom: `1px solid ${theme.border}` }}>
                        <span>Timestamp</span><span>Action</span><span style={{ textAlign: 'right' }}>Price</span><span style={{ textAlign: 'right' }}>Qty</span>
                      </div>
                      {trades.map(t => {
                        const isBuy = t.action?.includes('BOUGHT');
                        return (
                          <div key={t.id} className="row-hover" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 100px 80px', padding: '14px 16px', borderRadius: 8, fontSize: 13, backgroundColor: theme.bg, borderLeft: `4px solid ${isBuy ? theme.green : theme.red}`, alignItems: 'center' }}>
                            <span style={{ color: theme.textMuted }}>{new Date(t.timestamp).toLocaleTimeString()}</span>
                            <span style={{ color: isBuy ? theme.green : theme.red, fontWeight: 700 }}>{t.action}</span>
                            <span style={{ color: theme.text, textAlign: 'right', fontWeight: 500 }}>${fmt(t.price)}</span>
                            <span style={{ color: theme.textSub, textAlign: 'right' }}>{t.shares ?? 1}</span>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </Card>
              )}

              {/* ── BENCHMARK TAB (ACADEMIC PRESENTATION VIEW) ─────────────────────────── */}
              {activeTab === 'benchmark' && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 24, height: '100%', paddingBottom: 40 }}>
                  
                  {/* Header */}
                  <Card style={{ padding: '24px 32px' }}>
                    <div style={{ fontSize: 20, fontWeight: 700, color: theme.text, letterSpacing: '-0.02em', marginBottom: 8 }}>
                      System Performance & Parallel Efficiency Analysis
                    </div>
                    <div style={{ fontSize: 13, color: theme.textSub, lineHeight: 1.5, maxWidth: 800 }}>
                      Compares each benchmark against a single-core baseline using the same workload that the backend executes. In-memory tests use preloaded warm-cache data to isolate compute time from disk I/O.
                    </div>
                  </Card>

                  {/* CHANGED: gridTemplateColumns is now '1fr 1fr' for a perfect 2-column layout */}
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>

                    {/* NEW TEST 1: Streaming Latency (Formerly Test 2) */}
                    <Card style={{ gap: 16 }}>
                      <div style={{ borderBottom: `1px solid ${theme.border}`, paddingBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div>
                          <div style={{ fontSize: 11, color: theme.textSub, textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 700, marginBottom: 4 }}>Test 1: Real-Time</div>
                          <div style={{ fontSize: 15, fontWeight: 700, color: theme.text }}>Streaming Engine Latency</div>
                        </div>
                        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                          <button onClick={() => setOptStream(!optStream)}
                            style={{ padding: '6px 10px', borderRadius: 6, border: `1px solid ${optStream ? theme.purple : theme.border}`, backgroundColor: optStream ? `${theme.purple}15` : theme.bg, color: optStream ? theme.purple : theme.textMuted, fontWeight: 700, fontSize: 10, cursor: 'pointer', transition: 'all 0.2s' }}>
                            SIMD: {optStream ? 'ON' : 'OFF'}
                          </button>
                          <button className="btn-hover" onClick={() => triggerSpecificBenchmark('streaming', setBenchStream, optStream)} disabled={benchStream}
                            style={{ padding: '6px 12px', borderRadius: 6, border: `1px solid ${theme.green}`, backgroundColor: benchStream ? theme.surfaceHigh : `${theme.green}15`, color: theme.green, fontWeight: 600, fontSize: 11, cursor: benchStream ? 'wait' : 'pointer' }}>
                            {benchStream ? 'Running...' : 'Execute Test'}
                          </button>
                        </div>
                      </div>
                      
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                        <StatRow label="Active Stream" value={streamData ? `${streamData.symbols?.toLocaleString() ?? 500} Symbols` : '—'}/>
                        <StatRow label="Workload" value={streamData?.task ?? "HFT Burst Inference"}/>
                        <StatRow label="Mode" value={streamData?.model ?? (optStream ? "Tungsten SIMD Vectorization" : "Fair Distributed Chunking")}/>
                        <StatRow label="Measurement" value="Batch Runtime"/>
                      </div>

                      <div style={{ marginTop: 'auto', paddingTop: 16, borderTop: `1px solid ${theme.border}` }}>
                        <BenchmarkComparison
                          data={streamData}
                          serialKey="serial_ms"
                          parallelKey="parallel_ms"
                          unit="ms"
                          color={theme.green}
                          serialLabel="Sequential Loop"
                          parallelLabel={streamData?.model?.includes("SIMD") ? "Vectorized Batch" : "Parallel Workers"}
                        />
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8, fontSize: 12 }}>
                          <span style={{ color: theme.textSub }}>Sequential Python Loop</span>
                          <span style={{ color: theme.text, fontWeight: 600 }}>{streamData ? `${fmt(streamData.serial_ms, 1)}ms` : '—'}</span>
                        </div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16, fontSize: 12 }}>
                          <span style={{ color: theme.green, fontWeight: 600 }}>{streamData?.model?.includes("SIMD") ? "C++ Vectorized Batch" : "Parallel Workers"}</span>
                          <span style={{ color: theme.green, fontWeight: 700 }}>{streamData ? `${fmt(streamData.parallel_ms, 1)}ms` : '—'}</span>
                        </div>
                        
                        <div style={{ backgroundColor: theme.surfaceHigh, padding: 12, borderRadius: 8, textAlign: 'center', border: `1px solid ${theme.border}` }}>
                          <div style={{ fontSize: 24, fontWeight: 700, color: theme.green }}>{streamData?.speedup ?? '—'}x</div>
                          <div style={{ fontSize: 11, color: theme.textSub, textTransform: 'uppercase', letterSpacing: '0.05em', marginTop: 4, fontWeight: 600 }}>Latency Reduction</div>
                        </div>
                      </div>
                    </Card>

                    {/* NEW TEST 2: PySpark Architecture (Formerly Test 3) */}
                    <Card style={{ gap: 16 }}>
                      <div style={{ borderBottom: `1px solid ${theme.border}`, paddingBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div>
                          <div style={{ fontSize: 11, color: theme.textSub, textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 700, marginBottom: 4 }}>Test 2: Data Engine</div>
                          <div style={{ fontSize: 15, fontWeight: 700, color: theme.text }}>PySpark Parallelism</div>
                        </div>
                        <button className="btn-hover" onClick={() => triggerSpecificBenchmark('compute', setBenchCompute)} disabled={benchCompute}
                          style={{ padding: '6px 12px', borderRadius: 6, border: `1px solid ${theme.purple}`, backgroundColor: benchCompute ? theme.surfaceHigh : `${theme.purple}15`, color: theme.purple, fontWeight: 600, fontSize: 11, cursor: benchCompute ? 'wait' : 'pointer' }}>
                          {benchCompute ? 'Running...' : 'Execute Test'}
                        </button>
                      </div>
                      
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                        <StatRow label="Rows Shuffled" value={mcData ? (mcData.data_points).toLocaleString() : '—'}/>
                        <StatRow label="Workload" value={mcData?.task ?? "Heavy DataFrame Aggregation"}/>
                        <StatRow label="Engine" value={mcData?.model ?? "PySpark MapReduce"}/>
                        <StatRow label="Nodes/Cores" value={mcData?.cores_used ?? 'All'}/>
                      </div>

                      <div style={{ marginTop: 'auto', paddingTop: 16, borderTop: `1px solid ${theme.border}` }}>
                        <BenchmarkComparison
                          data={mcData}
                          serialKey="serial_time"
                          parallelKey="parallel_time"
                          unit="s"
                          color={theme.purple}
                          serialLabel="PySpark Serial"
                          parallelLabel="PySpark Parallel"
                        />
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8, fontSize: 12 }}>
                          <span style={{ color: theme.textSub }}>PySpark (local[1])</span>
                          <span style={{ color: theme.text, fontWeight: 600 }}>{mcData ? `${fmt(mcData.serial_time, 2)}s` : '—'}</span>
                        </div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16, fontSize: 12 }}>
                          <span style={{ color: theme.purple, fontWeight: 600 }}>PySpark (local[*])</span>
                          <span style={{ color: theme.purple, fontWeight: 700 }}>{mcData ? `${fmt(mcData.parallel_time, 2)}s` : '—'}</span>
                        </div>
                        
                        <div style={{ backgroundColor: theme.surfaceHigh, padding: 12, borderRadius: 8, textAlign: 'center', border: `1px solid ${theme.border}` }}>
                          <div style={{ fontSize: 24, fontWeight: 700, color: theme.purple }}>{mcData?.speedup ?? '—'}x</div>
                          <div style={{ fontSize: 11, color: theme.textSub, textTransform: 'uppercase', letterSpacing: '0.05em', marginTop: 4, fontWeight: 600 }}>Cluster Speedup</div>
                        </div>
                      </div>
                    </Card>
                    
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>

      <UnifiedConfigModal open={sysConfigOpen} onClose={() => setSysConfigOpen(false)} config={liveConfig} onConfigChange={setLiveConfig} onResetPortfolio={handleResetPortfolio} />
    </div>
  );
}

// ── Application Entry ──────────────────────────────────────────────────────────
export default function App() {
  return (
    <ThemeProvider>
      <GlobalStyles />
      <MainDashboard />
    </ThemeProvider>
  );
}