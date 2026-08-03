import { useState, useEffect, useRef, useCallback } from "react";
import { LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine, Legend, BarChart, Bar } from "recharts";

// ─── PALETTE ────────────────────────────────────────────────────────────────
const C = {
  bg: "#080c14",
  surface: "#0d1420",
  panel: "#111827",
  border: "#1e2d40",
  borderBright: "#243447",
  accent: "#00d4ff",
  accentDim: "#00a8cc",
  green: "#00e676",
  greenDim: "#00b85c",
  amber: "#ffb300",
  red: "#ff3d57",
  purple: "#9c6bff",
  text: "#e2e8f0",
  textMuted: "#64748b",
  textDim: "#94a3b8",
};

// ─── FONTS ──────────────────────────────────────────────────────────────────
const FONTS = `
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Syne:wght@400;500;600;700;800&display=swap');
`;

// ─── FAKE DATA GENERATORS ───────────────────────────────────────────────────
function seededRand(seed) {
  let s = seed;
  return () => { s = (s * 1664525 + 1013904223) & 0xffffffff; return (s >>> 0) / 0xffffffff; };
}

function genConsumptionData(hours = 48) {
  const r = seededRand(42);
  const now = Date.now();
  return Array.from({ length: hours }, (_, i) => {
    const t = new Date(now - (hours - i) * 3600000);
    const hour = t.getHours();
    const base = hour >= 7 && hour <= 22 ? 320 + Math.sin((hour - 7) / 15 * Math.PI) * 180 : 160;
    const noise = (r() - 0.5) * 60;
    return {
      time: t.toLocaleTimeString("en", { hour: "2-digit", minute: "2-digit" }),
      fullTime: t.toISOString(),
      kw: Math.max(80, Math.round(base + noise)),
      label: i % 6 === 0 ? t.toLocaleTimeString("en", { hour: "2-digit", minute: "2-digit" }) : "",
    };
  });
}

function genForecastData() {
  const r = seededRand(77);
  const now = Date.now();
  return Array.from({ length: 24 }, (_, i) => {
    const t = new Date(now + i * 3600000);
    const hour = t.getHours();
    const base = hour >= 7 && hour <= 22 ? 2800 + Math.sin((hour - 7) / 15 * Math.PI) * 1400 : 1200;
    const actual = i < 6 ? Math.round(base + (r() - 0.5) * 300) : null;
    const predicted = Math.round(base + (r() - 0.5) * 200);
    return {
      time: t.toLocaleTimeString("en", { hour: "2-digit", minute: "2-digit" }),
      actual,
      predicted,
      upper: predicted + Math.round(r() * 200 + 80),
      lower: predicted - Math.round(r() * 200 + 80),
    };
  });
}

const ANOMALY_TYPES = ["Sudden Spike", "Sustained Elevation", "Flatline"];
const ZONES = ["Zone A - North", "Zone B - Central", "Zone C - South", "Zone D - East", "Zone E - West"];
const METERS = Array.from({ length: 20 }, (_, i) => `HH-${String(i + 1001).padStart(5, "0")}`);

function genAlerts(count = 12) {
  const r = seededRand(55);
  const types = ["Sudden Spike", "Sustained Elevation", "Flatline"];
  const severities = ["Critical", "High", "Medium", "Low"];
  const sevColors = { Critical: C.red, High: C.amber, Medium: "#f59e0b", Low: C.green };
  const now = Date.now();
  return Array.from({ length: count }, (_, i) => {
    const type = types[Math.floor(r() * 3)];
    const sev = severities[Math.floor(r() * 4)];
    const minsAgo = Math.floor(r() * 120);
    return {
      id: `ALT-${1000 + i}`,
      meter: METERS[Math.floor(r() * METERS.length)],
      type,
      severity: sev,
      sevColor: sevColors[sev],
      zscore: (2.5 + r() * 4).toFixed(2),
      zone: ZONES[Math.floor(r() * ZONES.length)],
      time: new Date(now - minsAgo * 60000).toLocaleTimeString("en", { hour: "2-digit", minute: "2-digit" }),
      minsAgo,
      status: r() > 0.6 ? "Open" : "Resolved",
    };
  });
}

function genHouseholdData() {
  const r = seededRand(99);
  return METERS.slice(0, 10).map((m, i) => ({
    id: m,
    zone: ZONES[i % ZONES.length],
    anomalies: Math.floor(r() * 15 + 1),
    avgKwh: (r() * 8 + 1.5).toFixed(2),
    zscore: (r() * 5 + 2).toFixed(2),
    status: r() > 0.5 ? "Critical" : r() > 0.3 ? "Warning" : "Normal",
  }));
}

function genMeterHistory(meterId) {
  const r = seededRand(meterId.charCodeAt(3) * 7);
  const now = Date.now();
  const anomalyPoints = new Set([8, 16, 24, 38, 44]);
  return Array.from({ length: 48 }, (_, i) => {
    const t = new Date(now - (48 - i) * 1800000);
    const hour = t.getHours();
    const base = hour >= 7 && hour <= 22 ? 3.2 + Math.sin((hour - 7) / 15 * Math.PI) * 2 : 1.5;
    const isAnomaly = anomalyPoints.has(i);
    const val = isAnomaly ? base * (2.5 + r() * 1.5) : base + (r() - 0.5) * 0.8;
    return {
      time: t.toLocaleTimeString("en", { hour: "2-digit", minute: "2-digit" }),
      kwh: Math.max(0.1, +val.toFixed(3)),
      zscore: isAnomaly ? +(2.5 + r() * 4).toFixed(2) : +(r() * 1.5).toFixed(2),
      anomaly: isAnomaly,
    };
  });
}

// ─── STATIC DATA ─────────────────────────────────────────────────────────────
const CONSUMPTION = genConsumptionData(48);
const FORECAST = genForecastData();
const INITIAL_ALERTS = genAlerts(12);
const HOUSEHOLDS = genHouseholdData();

// ─── STYLES ──────────────────────────────────────────────────────────────────
const css = `
${FONTS}
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body, #root { background: ${C.bg}; color: ${C.text}; font-family: 'Syne', sans-serif; min-height: 100vh; }
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: ${C.surface}; }
::-webkit-scrollbar-thumb { background: ${C.border}; border-radius: 3px; }

.mono { font-family: 'JetBrains Mono', monospace; }

.sidebar { position: fixed; left: 0; top: 0; height: 100vh; width: 220px; background: ${C.surface}; border-right: 1px solid ${C.border}; display: flex; flex-direction: column; z-index: 100; }
.sidebar-logo { padding: 20px 20px 16px; border-bottom: 1px solid ${C.border}; }
.sidebar-logo .logo-text { font-size: 13px; font-weight: 700; color: ${C.accent}; letter-spacing: 0.08em; text-transform: uppercase; }
.sidebar-logo .logo-sub { font-size: 10px; color: ${C.textMuted}; margin-top: 2px; letter-spacing: 0.05em; }
.sidebar-logo .logo-icon { width: 32px; height: 32px; margin-bottom: 8px; }
.nav-section { padding: 12px 0; flex: 1; overflow-y: auto; }
.nav-label { font-size: 10px; color: ${C.textMuted}; letter-spacing: 0.12em; text-transform: uppercase; padding: 4px 20px 6px; }
.nav-item { display: flex; align-items: center; gap: 10px; padding: 9px 20px; cursor: pointer; font-size: 13px; color: ${C.textDim}; transition: all 0.15s; border-left: 2px solid transparent; }
.nav-item:hover { color: ${C.text}; background: rgba(0,212,255,0.04); }
.nav-item.active { color: ${C.accent}; background: rgba(0,212,255,0.08); border-left-color: ${C.accent}; }
.nav-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
.sidebar-footer { padding: 14px 20px; border-top: 1px solid ${C.border}; font-size: 11px; color: ${C.textMuted}; }

.main { margin-left: 220px; min-height: 100vh; }
.topbar { height: 56px; border-bottom: 1px solid ${C.border}; display: flex; align-items: center; justify-content: space-between; padding: 0 28px; background: ${C.surface}; position: sticky; top: 0; z-index: 50; }
.topbar-title { font-size: 15px; font-weight: 600; color: ${C.text}; }
.topbar-right { display: flex; align-items: center; gap: 16px; }
.status-pill { display: flex; align-items: center; gap: 6px; padding: 4px 10px; border-radius: 20px; border: 1px solid; font-size: 11px; font-weight: 600; letter-spacing: 0.05em; }
.status-pill.live { border-color: rgba(0,230,118,0.4); color: ${C.green}; background: rgba(0,230,118,0.08); }
.pulse { width: 7px; height: 7px; border-radius: 50%; background: ${C.green}; animation: pulse 1.5s ease-in-out infinite; }
@keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.5; transform: scale(0.8); } }

.page-content { padding: 24px 28px; }
.page-header { margin-bottom: 24px; }
.page-header h1 { font-size: 22px; font-weight: 700; color: ${C.text}; }
.page-header p { font-size: 13px; color: ${C.textMuted}; margin-top: 4px; }

.metric-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 20px; }
.metric-card { background: ${C.panel}; border: 1px solid ${C.border}; border-radius: 10px; padding: 16px 18px; position: relative; overflow: hidden; }
.metric-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px; }
.metric-card.blue::before { background: linear-gradient(90deg, ${C.accent}, transparent); }
.metric-card.green::before { background: linear-gradient(90deg, ${C.green}, transparent); }
.metric-card.red::before { background: linear-gradient(90deg, ${C.red}, transparent); }
.metric-card.amber::before { background: linear-gradient(90deg, ${C.amber}, transparent); }
.metric-label { font-size: 11px; color: ${C.textMuted}; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 8px; }
.metric-value { font-size: 26px; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
.metric-value.blue { color: ${C.accent}; }
.metric-value.green { color: ${C.green}; }
.metric-value.red { color: ${C.red}; }
.metric-value.amber { color: ${C.amber}; }
.metric-sub { font-size: 11px; color: ${C.textMuted}; margin-top: 6px; }
.metric-trend { font-size: 11px; margin-top: 4px; }
.metric-trend.up { color: ${C.red}; }
.metric-trend.down { color: ${C.green}; }

.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
.grid-3-1 { display: grid; grid-template-columns: 2fr 1fr; gap: 16px; margin-bottom: 16px; }

.panel { background: ${C.panel}; border: 1px solid ${C.border}; border-radius: 10px; overflow: hidden; }
.panel-header { display: flex; align-items: center; justify-content: space-between; padding: 14px 18px; border-bottom: 1px solid ${C.border}; }
.panel-title { font-size: 13px; font-weight: 600; color: ${C.text}; display: flex; align-items: center; gap: 8px; }
.panel-badge { font-size: 10px; padding: 2px 7px; border-radius: 10px; font-family: 'JetBrains Mono', monospace; font-weight: 600; }
.panel-badge.red { background: rgba(255,61,87,0.15); color: ${C.red}; border: 1px solid rgba(255,61,87,0.25); }
.panel-badge.green { background: rgba(0,230,118,0.12); color: ${C.green}; border: 1px solid rgba(0,230,118,0.2); }
.panel-badge.blue { background: rgba(0,212,255,0.12); color: ${C.accent}; border: 1px solid rgba(0,212,255,0.2); }
.panel-body { padding: 16px 18px; }

.alert-list { display: flex; flex-direction: column; gap: 0; }
.alert-item { display: flex; align-items: center; gap: 12px; padding: 10px 18px; border-bottom: 1px solid rgba(30,45,64,0.7); transition: background 0.12s; cursor: pointer; }
.alert-item:hover { background: rgba(255,255,255,0.02); }
.alert-item:last-child { border-bottom: none; }
.alert-sev-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.alert-info { flex: 1; min-width: 0; }
.alert-meter { font-size: 12px; font-weight: 600; font-family: 'JetBrains Mono', monospace; color: ${C.text}; }
.alert-type { font-size: 11px; color: ${C.textDim}; }
.alert-meta { text-align: right; font-size: 11px; }
.alert-time { color: ${C.textMuted}; }
.alert-sev { font-size: 10px; font-weight: 700; margin-top: 2px; }
.alert-zscore { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: ${C.textMuted}; min-width: 60px; text-align: center; }
.alert-status-tag { font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 4px; }

.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
thead th { font-size: 10px; text-transform: uppercase; letter-spacing: 0.08em; color: ${C.textMuted}; padding: 10px 14px; border-bottom: 1px solid ${C.border}; text-align: left; font-weight: 600; }
tbody td { padding: 10px 14px; border-bottom: 1px solid rgba(30,45,64,0.5); color: ${C.textDim}; }
tbody tr:last-child td { border-bottom: none; }
tbody tr:hover td { background: rgba(255,255,255,0.018); color: ${C.text}; }
.td-mono { font-family: 'JetBrains Mono', monospace; font-size: 11px; }
.status-badge { font-size: 10px; font-weight: 700; padding: 2px 7px; border-radius: 4px; display: inline-block; }
.status-badge.critical { background: rgba(255,61,87,0.15); color: ${C.red}; }
.status-badge.warning { background: rgba(255,179,0,0.15); color: ${C.amber}; }
.status-badge.normal { background: rgba(0,230,118,0.12); color: ${C.green}; }

.filter-row { display: flex; align-items: center; gap: 10px; margin-bottom: 18px; flex-wrap: wrap; }
.filter-select { background: ${C.surface}; border: 1px solid ${C.border}; color: ${C.text}; font-family: 'Syne', sans-serif; font-size: 12px; padding: 6px 10px; border-radius: 6px; outline: none; cursor: pointer; }
.filter-select:focus { border-color: ${C.accent}; }
.filter-label { font-size: 12px; color: ${C.textMuted}; }

.arch-diagram { padding: 24px 18px; }

.pipeline-row { display: flex; align-items: center; gap: 0; margin-bottom: 28px; }
.pipe-node { background: ${C.surface}; border: 1px solid ${C.border}; border-radius: 8px; padding: 10px 14px; text-align: center; min-width: 100px; position: relative; }
.pipe-node.active { border-color: ${C.accent}; box-shadow: 0 0 0 1px rgba(0,212,255,0.15); }
.pipe-node-title { font-size: 11px; font-weight: 700; color: ${C.accent}; }
.pipe-node-sub { font-size: 10px; color: ${C.textMuted}; margin-top: 2px; }
.pipe-arrow { flex: 1; height: 1px; background: linear-gradient(90deg, ${C.accentDim}, ${C.accent}); position: relative; display: flex; align-items: center; justify-content: center; min-width: 20px; }
.pipe-arrow::after { content: '▶'; color: ${C.accent}; font-size: 9px; position: absolute; }

.cluster-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }
.cluster-card { background: ${C.surface}; border: 1px solid ${C.border}; border-radius: 10px; padding: 16px; }
.cluster-card.a { border-top: 2px solid ${C.accent}; }
.cluster-card.b { border-top: 2px solid ${C.green}; }
.cluster-card.c { border-top: 2px solid ${C.purple}; }
.cluster-title { font-size: 12px; font-weight: 700; margin-bottom: 6px; }
.cluster-title.a { color: ${C.accent}; }
.cluster-title.b { color: ${C.green}; }
.cluster-title.c { color: ${C.purple}; }
.cluster-sub { font-size: 11px; color: ${C.textMuted}; margin-bottom: 10px; }
.cluster-tag { display: inline-block; font-size: 10px; font-family: 'JetBrains Mono', monospace; padding: 2px 7px; border-radius: 4px; margin: 2px; }
.cluster-tag.a { background: rgba(0,212,255,0.1); color: ${C.accent}; }
.cluster-tag.b { background: rgba(0,230,118,0.1); color: ${C.green}; }
.cluster-tag.c { background: rgba(156,107,255,0.1); color: ${C.purple}; }

.anomaly-dot { width: 8px; height: 8px; border-radius: 50%; background: ${C.red}; display: inline-block; animation: pulse 1.2s ease-in-out infinite; }

.metric-forecast-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 20px; }

.btn { padding: 7px 14px; border-radius: 6px; font-family: 'Syne', sans-serif; font-size: 12px; font-weight: 600; cursor: pointer; border: 1px solid; transition: all 0.15s; }
.btn-primary { background: rgba(0,212,255,0.12); color: ${C.accent}; border-color: rgba(0,212,255,0.3); }
.btn-primary:hover { background: rgba(0,212,255,0.2); }

.landing { min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; position: relative; overflow: hidden; background: ${C.bg}; }
.landing-grid { position: absolute; inset: 0; background-image: linear-gradient(rgba(0,212,255,0.04) 1px, transparent 1px), linear-gradient(90deg, rgba(0,212,255,0.04) 1px, transparent 1px); background-size: 48px 48px; }
.landing-glow { position: absolute; width: 600px; height: 600px; border-radius: 50%; background: radial-gradient(circle, rgba(0,212,255,0.07) 0%, transparent 70%); top: 50%; left: 50%; transform: translate(-50%, -50%); pointer-events: none; }
.landing-content { position: relative; text-align: center; max-width: 700px; padding: 40px; }
.landing-badge { display: inline-flex; align-items: center; gap: 6px; border: 1px solid rgba(0,212,255,0.3); border-radius: 20px; padding: 5px 14px; font-size: 11px; color: ${C.accent}; letter-spacing: 0.08em; text-transform: uppercase; margin-bottom: 24px; background: rgba(0,212,255,0.06); }
.landing-h1 { font-size: 52px; font-weight: 800; line-height: 1.1; color: ${C.text}; margin-bottom: 16px; }
.landing-h1 span { color: ${C.accent}; }
.landing-sub { font-size: 16px; color: ${C.textDim}; line-height: 1.6; margin-bottom: 36px; max-width: 500px; margin-left: auto; margin-right: auto; }
.landing-cta { display: inline-flex; align-items: center; gap: 10px; padding: 14px 32px; background: ${C.accent}; color: ${C.bg}; font-family: 'Syne', sans-serif; font-size: 14px; font-weight: 700; border-radius: 8px; cursor: pointer; border: none; letter-spacing: 0.04em; transition: all 0.2s; }
.landing-cta:hover { background: ${C.accentDim}; transform: translateY(-1px); }
.landing-stats { display: flex; gap: 48px; margin-top: 52px; justify-content: center; }
.landing-stat-val { font-size: 28px; font-weight: 700; font-family: 'JetBrains Mono', monospace; color: ${C.accent}; }
.landing-stat-label { font-size: 11px; color: ${C.textMuted}; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.06em; }
.landing-particles { position: absolute; inset: 0; pointer-events: none; }

@media (max-width: 900px) {
  .sidebar { width: 180px; }
  .main { margin-left: 180px; }
  .metric-grid { grid-template-columns: repeat(2, 1fr); }
  .grid-2 { grid-template-columns: 1fr; }
  .grid-3-1 { grid-template-columns: 1fr; }
  .cluster-grid { grid-template-columns: 1fr; }
}
`;

// ─── COMPONENTS ──────────────────────────────────────────────────────────────
function PulseOrb({ color = C.green, size = 7 }) {
  return <span style={{ width: size, height: size, borderRadius: "50%", background: color, display: "inline-block", animation: "pulse 1.5s ease-in-out infinite", flexShrink: 0 }} />;
}

function MetricCard({ label, value, unit = "", color = "blue", sub, trend }) {
  return (
    <div className={`metric-card ${color}`}>
      <div className="metric-label">{label}</div>
      <div className={`metric-value ${color}`}>{value}<span style={{ fontSize: 14, fontWeight: 400, marginLeft: 3 }}>{unit}</span></div>
      {sub && <div className="metric-sub">{sub}</div>}
      {trend && <div className={`metric-trend ${trend.dir}`}>{trend.text}</div>}
    </div>
  );
}

function Panel({ title, badge, badgeType = "blue", children, action, style }) {
  return (
    <div className="panel" style={style}>
      <div className="panel-header">
        <div className="panel-title">
          {title}
          {badge && <span className={`panel-badge ${badgeType}`}>{badge}</span>}
        </div>
        {action}
      </div>
      {children}
    </div>
  );
}

const CustomTooltip = ({ active, payload, label, prefix = "", suffix = "" }) => {
  if (active && payload && payload.length) {
    return (
      <div style={{ background: C.panel, border: `1px solid ${C.border}`, borderRadius: 6, padding: "8px 12px", fontSize: 11, fontFamily: "'JetBrains Mono', monospace" }}>
        <div style={{ color: C.textMuted, marginBottom: 4 }}>{label}</div>
        {payload.map(p => (
          <div key={p.name} style={{ color: p.color, marginTop: 2 }}>
            {p.name}: {prefix}{p.value}{suffix}
          </div>
        ))}
      </div>
    );
  }
  return null;
};

// ─── PAGES ───────────────────────────────────────────────────────────────────
function Landing({ onEnter }) {
  return (
    <div className="landing">
      <div className="landing-grid" />
      <div className="landing-glow" />
      <div className="landing-content">
        <div className="landing-badge">
          <PulseOrb size={6} /> Live monitoring active
        </div>
        <h1 className="landing-h1">
          Smart Grid<br /><span>Energy Analytics</span>
        </h1>
        <p className="landing-sub">
          Real-time anomaly detection & 24-hour demand forecasting for city-scale electricity networks.
        </p>
        <button className="landing-cta" onClick={onEnter}>
          Open Dashboard <span style={{ fontSize: 16 }}>→</span>
        </button>
        <div className="landing-stats">
          {[["100K+", "Smart Meters"], ["4.8M", "Readings / Day"], ["< 2s", "Alert Latency"], ["96B$", "Theft Prevented"]].map(([v, l]) => (
            <div key={l}>
              <div className="landing-stat-val">{v}</div>
              <div className="landing-stat-label">{l}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function Dashboard({ alerts, consumption, metrics }) {
  const CONS = consumption.slice(-24);
  return (
    <div className="page-content">
      <div className="page-header">
        <h1>Operations Overview</h1>
        <p>Live grid monitoring — last updated just now</p>
      </div>

      <div className="metric-grid">
        <MetricCard label="Total Consumption" value={metrics.totalKw.toLocaleString()} unit="kW" color="blue" sub="Grid-wide current draw" trend={{ dir: "up", text: "↑ 4.2% vs last hour" }} />
        <MetricCard label="Active Meters" value={metrics.activeMeters.toLocaleString()} color="green" sub="of 100,000 deployed" trend={{ dir: "down", text: "↓ 12 offline" }} />
        <MetricCard label="Alerts (1h)" value={metrics.alertsHour} color="red" sub="Anomalies flagged" trend={{ dir: "up", text: "↑ 3 new since last check" }} />
        <MetricCard label="Forecast MAPE" value="3.7" unit="%" color="amber" sub="24h demand model accuracy" trend={{ dir: "down", text: "↓ 0.2% improvement" }} />
      </div>

      <div className="grid-3-1">
        <Panel title="Energy Consumption" badge="Live" badgeType="green">
          <div style={{ padding: "16px 8px 8px" }}>
            <ResponsiveContainer width="100%" height={200}>
              <AreaChart data={CONS}>
                <defs>
                  <linearGradient id="kwGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={C.accent} stopOpacity={0.2} />
                    <stop offset="95%" stopColor={C.accent} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                <XAxis dataKey="time" tick={{ fill: C.textMuted, fontSize: 10, fontFamily: "JetBrains Mono" }} tickLine={false} interval={5} />
                <YAxis tick={{ fill: C.textMuted, fontSize: 10, fontFamily: "JetBrains Mono" }} tickLine={false} axisLine={false} />
                <Tooltip content={<CustomTooltip suffix=" kW" />} />
                <Area type="monotone" dataKey="kw" name="Consumption" stroke={C.accent} strokeWidth={1.5} fill="url(#kwGrad)" dot={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel title="Anomaly Types" badge={`${alerts.filter(a => a.status === "Open").length} open`} badgeType="red">
          <div style={{ padding: "16px 18px" }}>
            {["Sudden Spike", "Sustained Elevation", "Flatline"].map((type, i) => {
              const cnt = alerts.filter(a => a.type === type).length;
              const colors = [C.red, C.amber, C.purple];
              const pct = Math.round(cnt / alerts.length * 100);
              return (
                <div key={type} style={{ marginBottom: 14 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, marginBottom: 5 }}>
                    <span style={{ color: C.textDim }}>{type}</span>
                    <span style={{ fontFamily: "JetBrains Mono", color: colors[i] }}>{cnt}</span>
                  </div>
                  <div style={{ height: 4, background: C.surface, borderRadius: 2 }}>
                    <div style={{ height: "100%", width: `${pct}%`, background: colors[i], borderRadius: 2, transition: "width 0.5s" }} />
                  </div>
                </div>
              );
            })}
            <div style={{ marginTop: 20, paddingTop: 14, borderTop: `1px solid ${C.border}` }}>
              <div style={{ fontSize: 10, color: C.textMuted, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.06em" }}>Zone Distribution</div>
              {ZONES.slice(0, 3).map(z => {
                const cnt = alerts.filter(a => a.zone === z).length;
                return (
                  <div key={z} style={{ display: "flex", justifyContent: "space-between", fontSize: 11, padding: "4px 0", borderBottom: `1px solid ${C.border}` }}>
                    <span style={{ color: C.textDim }}>{z}</span>
                    <span style={{ fontFamily: "JetBrains Mono", fontSize: 11, color: C.accent }}>{cnt}</span>
                  </div>
                );
              })}
            </div>
          </div>
        </Panel>
      </div>

      <div className="grid-2" style={{ marginBottom: 16 }}>
        <Panel title="Live Anomaly Alerts" badge={`${alerts.filter(a => a.status === "Open").length} Open`} badgeType="red"
          action={<span style={{ fontSize: 10, color: C.textMuted, cursor: "pointer", padding: "3px 8px", border: `1px solid ${C.border}`, borderRadius: 4 }}>View All</span>}>
          <div className="alert-list">
            {alerts.slice(0, 6).map(a => (
              <div className="alert-item" key={a.id}>
                <div className="alert-sev-dot" style={{ background: a.sevColor }} />
                <div className="alert-info">
                  <div className="alert-meter">{a.meter}</div>
                  <div className="alert-type">{a.type} · {a.zone}</div>
                </div>
                <div className="alert-zscore mono">z={a.zscore}</div>
                <div className="alert-meta">
                  <div className="alert-time">{a.minsAgo}m ago</div>
                  <div className="alert-sev" style={{ color: a.sevColor }}>{a.severity}</div>
                </div>
                <span className="alert-status-tag" style={a.status === "Open" ? { background: "rgba(255,61,87,0.12)", color: C.red } : { background: "rgba(0,230,118,0.1)", color: C.green }}>{a.status}</span>
              </div>
            ))}
          </div>
        </Panel>

        <Panel title="Top Anomalous Households">
          <div className="table-wrap">
            <table>
              <thead>
                <tr><th>Meter ID</th><th>Zone</th><th>Alerts</th><th>Z-Score</th><th>Status</th></tr>
              </thead>
              <tbody>
                {HOUSEHOLDS.slice(0, 7).map(h => (
                  <tr key={h.id}>
                    <td className="td-mono" style={{ color: C.accent }}>{h.id}</td>
                    <td style={{ fontSize: 11 }}>{h.zone.split(" - ")[0]}</td>
                    <td className="td-mono">{h.anomalies}</td>
                    <td className="td-mono">{h.zscore}</td>
                    <td><span className={`status-badge ${h.status.toLowerCase()}`}>{h.status}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      </div>
    </div>
  );
}

function AnomalyPage({ alerts }) {
  const [selectedMeter, setSelectedMeter] = useState(METERS[0]);
  const [filterZone, setFilterZone] = useState("All Zones");
  const history = genMeterHistory(selectedMeter);

  const filteredAlerts = alerts.filter(a =>
    (filterZone === "All Zones" || a.zone === filterZone) &&
    a.meter === selectedMeter
  );

  return (
    <div className="page-content">
      <div className="page-header">
        <h1>Anomaly Details</h1>
        <p>Per-meter consumption analysis with Z-score anomaly highlights</p>
      </div>

      <div className="filter-row">
        <span className="filter-label">Meter ID:</span>
        <select className="filter-select" value={selectedMeter} onChange={e => setSelectedMeter(e.target.value)}>
          {METERS.map(m => <option key={m}>{m}</option>)}
        </select>
        <span className="filter-label">Zone:</span>
        <select className="filter-select" value={filterZone} onChange={e => setFilterZone(e.target.value)}>
          <option>All Zones</option>
          {ZONES.map(z => <option key={z}>{z}</option>)}
        </select>
      </div>

      <div className="metric-grid" style={{ gridTemplateColumns: "repeat(4, 1fr)" }}>
        <MetricCard label="Anomalies (48h)" value={history.filter(h => h.anomaly).length} color="red" />
        <MetricCard label="Max Z-Score" value={Math.max(...history.map(h => h.zscore)).toFixed(2)} color="amber" />
        <MetricCard label="Avg kWh/30min" value={(history.reduce((s, h) => s + h.kwh, 0) / history.length).toFixed(3)} color="blue" />
        <MetricCard label="Last Anomaly" value={history.slice().reverse().find(h => h.anomaly)?.time || "None"} color="green" />
      </div>

      <Panel title={`Consumption — Last 48h · ${selectedMeter}`} badge={`${history.filter(h => h.anomaly).length} anomalies`} badgeType="red" style={{ marginBottom: 16 }}>
        <div style={{ padding: "16px 8px 8px" }}>
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={history}>
              <defs>
                <linearGradient id="kwhGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={C.accent} stopOpacity={0.15} />
                  <stop offset="95%" stopColor={C.accent} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
              <XAxis dataKey="time" tick={{ fill: C.textMuted, fontSize: 10 }} interval={7} tickLine={false} />
              <YAxis tick={{ fill: C.textMuted, fontSize: 10 }} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip suffix=" kWh" />} />
              <Area type="monotone" dataKey="kwh" name="kWh/30min" stroke={C.accent} strokeWidth={1.5} fill="url(#kwhGrad)" dot={(props) => {
                const { cx, cy, payload } = props;
                if (!payload.anomaly) return null;
                return <circle key={`dot-${cx}-${cy}`} cx={cx} cy={cy} r={5} fill={C.red} stroke={C.red} strokeWidth={2} opacity={0.9} />;
              }} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </Panel>

      <Panel title="Z-Score Timeline" badge="Threshold: ±3.0" badgeType="blue">
        <div style={{ padding: "16px 8px 8px" }}>
          <ResponsiveContainer width="100%" height={160}>
            <AreaChart data={history}>
              <defs>
                <linearGradient id="zGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={C.purple} stopOpacity={0.25} />
                  <stop offset="95%" stopColor={C.purple} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
              <XAxis dataKey="time" tick={{ fill: C.textMuted, fontSize: 10 }} interval={7} tickLine={false} />
              <YAxis tick={{ fill: C.textMuted, fontSize: 10 }} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip />} />
              <ReferenceLine y={3} stroke={C.red} strokeDasharray="4 4" label={{ value: "3σ", fill: C.red, fontSize: 10 }} />
              <ReferenceLine y={-3} stroke={C.red} strokeDasharray="4 4" />
              <Area type="monotone" dataKey="zscore" name="Z-Score" stroke={C.purple} strokeWidth={1.5} fill="url(#zGrad)" dot={false} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </Panel>
    </div>
  );
}

function ForecastPage() {
  const data = FORECAST;
  const actual = data.filter(d => d.actual !== null);
  const rmse = Math.sqrt(actual.reduce((s, d) => s + Math.pow(d.predicted - d.actual, 2), 0) / actual.length);
  const mae = actual.reduce((s, d) => s + Math.abs(d.predicted - d.actual), 0) / actual.length;
  const mape = actual.reduce((s, d) => s + Math.abs((d.predicted - d.actual) / d.actual), 0) / actual.length * 100;

  return (
    <div className="page-content">
      <div className="page-header">
        <h1>Demand Forecasting</h1>
        <p>GBT Regressor — 24-hour ahead prediction · PJM regional zones</p>
      </div>

      <div className="metric-forecast-grid">
        <MetricCard label="RMSE" value={rmse.toFixed(1)} unit="MW" color="blue" sub="Root Mean Squared Error" />
        <MetricCard label="MAE" value={mae.toFixed(1)} unit="MW" color="green" sub="Mean Absolute Error" />
        <MetricCard label="MAPE" value={mape.toFixed(2)} unit="%" color="amber" sub="Mean Absolute % Error" />
      </div>

      <Panel title="Predicted vs Actual Demand — Next 24h" badge="GBT Regressor" badgeType="blue" style={{ marginBottom: 16 }}>
        <div style={{ padding: "16px 8px 8px" }}>
          <div style={{ display: "flex", gap: 16, padding: "0 18px 10px", flexWrap: "wrap" }}>
            {[["Predicted", C.accent], ["Actual", C.green], ["Confidence Band", C.purple]].map(([label, color]) => (
              <div key={label} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: C.textMuted }}>
                <span style={{ width: 14, height: 2, background: color, display: "inline-block", borderRadius: 1 }} />
                {label}
              </div>
            ))}
          </div>
          <ResponsiveContainer width="100%" height={240}>
            <AreaChart data={data}>
              <defs>
                <linearGradient id="confGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={C.purple} stopOpacity={0.12} />
                  <stop offset="95%" stopColor={C.purple} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
              <XAxis dataKey="time" tick={{ fill: C.textMuted, fontSize: 10 }} tickLine={false} interval={3} />
              <YAxis tick={{ fill: C.textMuted, fontSize: 10 }} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip suffix=" MW" />} />
              <Area type="monotone" dataKey="upper" stroke="none" fill="url(#confGrad)" name="Upper CI" />
              <Line type="monotone" dataKey="predicted" stroke={C.accent} strokeWidth={2} dot={false} name="Predicted" />
              <Line type="monotone" dataKey="actual" stroke={C.green} strokeWidth={2} dot={{ fill: C.green, r: 3 }} name="Actual" connectNulls={false} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </Panel>

      <Panel title="Hourly Demand Pattern by Zone">
        <div style={{ padding: "16px 8px 8px" }}>
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={data.slice(0, 12)}>
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
              <XAxis dataKey="time" tick={{ fill: C.textMuted, fontSize: 10 }} tickLine={false} />
              <YAxis tick={{ fill: C.textMuted, fontSize: 10 }} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip suffix=" MW" />} />
              <Bar dataKey="predicted" fill={C.accent} name="Predicted" radius={[2, 2, 0, 0]} fillOpacity={0.8} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Panel>
    </div>
  );
}

function ArchitecturePage() {
  const pipeline = [
    { label: "Smart Meters", sub: "100K devices · 30min" },
    { label: "Apache Kafka", sub: "meter-readings topic" },
    { label: "Spark Streaming", sub: "Cluster A · <2s" },
    { label: "HDFS / HBase", sub: "Cluster B · Parquet" },
    { label: "Hive + Spark", sub: "Cluster C · Batch" },
    { label: "Superset", sub: "Dashboard layer" },
  ];

  const clusters = [
    {
      id: "a", label: "Cluster A", title: "Streaming & Ingestion",
      nodes: "1 Master + 2 Workers",
      desc: "Receives simulated meter events via Kafka; runs ML inference on each micro-batch; classifies anomalies in near-real-time.",
      tags: ["Kafka Broker", "Spark Streaming", "Spark MLlib", "Zookeeper"],
    },
    {
      id: "b", label: "Cluster B", title: "Storage & Query",
      nodes: "1 Master + 2 Workers",
      desc: "Persists all meter readings in HDFS Parquet; stores per-meter anomaly state and alert log in HBase; serves fast random-read requests.",
      tags: ["HDFS NameNode", "HBase Master", "DataNodes", "RegionServers"],
    },
    {
      id: "c", label: "Cluster C", title: "Analytics & Forecasting",
      nodes: "1 Master + 2 Workers",
      desc: "Runs Pig ETL scripts; trains GBT demand forecast model; executes Hive SQL for historical reporting; hosts Superset dashboard.",
      tags: ["Hive Metastore", "Spark Batch", "Apache Pig", "Superset"],
    },
  ];

  return (
    <div className="page-content">
      <div className="page-header">
        <h1>Data Pipeline & Architecture</h1>
        <p>Hadoop-centric distributed processing across 3 logical clusters</p>
      </div>

      <Panel title="Data Flow Pipeline" badge="End-to-End" badgeType="blue" style={{ marginBottom: 20 }}>
        <div className="arch-diagram">
          <div className="pipeline-row" style={{ flexWrap: "wrap", gap: 6 }}>
            {pipeline.map((n, i) => (
              <>
                <div key={n.label} className={`pipe-node ${i > 0 ? "active" : ""}`}>
                  <div className="pipe-node-title">{n.label}</div>
                  <div className="pipe-node-sub">{n.sub}</div>
                </div>
                {i < pipeline.length - 1 && <div className="pipe-arrow" key={`arr-${i}`} />}
              </>
            ))}
          </div>
          <div style={{ marginTop: 20, padding: "12px 16px", background: C.surface, borderRadius: 8, border: `1px solid ${C.border}` }}>
            <div style={{ fontSize: 11, color: C.textMuted, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.07em" }}>Data Flow Summary</div>
            <div style={{ fontSize: 12, color: C.textDim, lineHeight: 1.7 }}>
              Python Kafka producer replays London Smart Meters dataset at accelerated rate (1 day / 10 sec).
              Cluster A's Spark Streaming consumes the topic, extracts features, runs anomaly model, writes to HDFS &amp; HBase.
              Cluster C periodically refreshes demand forecast model via Spark MLlib on PJM dataset.
            </div>
          </div>
        </div>
      </Panel>

      <div style={{ marginBottom: 12, fontSize: 13, fontWeight: 600, color: C.textDim }}>Three-Cluster Deployment</div>
      <div className="cluster-grid" style={{ marginBottom: 20 }}>
        {clusters.map(cl => (
          <div key={cl.id} className={`cluster-card ${cl.id}`}>
            <div className={`cluster-title ${cl.id}`}>Cluster {cl.id.toUpperCase()} — {cl.title}</div>
            <div className="cluster-sub">{cl.nodes}</div>
            <div style={{ fontSize: 11, color: C.textMuted, lineHeight: 1.6, marginBottom: 10 }}>{cl.desc}</div>
            <div>{cl.tags.map(t => <span key={t} className={`cluster-tag ${cl.id}`}>{t}</span>)}</div>
          </div>
        ))}
      </div>

      <Panel title="Technology Stack" badge="Open Source" badgeType="green">
        <div style={{ padding: "16px 18px" }}>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
            {[
              ["Apache Hadoop", "HDFS + YARN — distributed storage & resource mgmt", C.accent],
              ["Apache Spark", "Batch + Structured Streaming compute backbone", C.accent],
              ["Apache Hive", "SQL abstraction over HDFS Parquet data lake", C.green],
              ["Apache HBase", "Millisecond-latency operational alert store", C.green],
              ["Apache Pig", "ETL pipeline for raw CSV cleaning & loading", C.purple],
              ["Apache Kafka", "Message broker decoupling meter simulation", C.amber],
            ].map(([name, desc, color]) => (
              <div key={name} style={{ padding: 12, background: C.surface, borderRadius: 8, border: `1px solid ${C.border}` }}>
                <div style={{ fontSize: 12, fontWeight: 700, color, marginBottom: 4 }}>{name}</div>
                <div style={{ fontSize: 11, color: C.textMuted, lineHeight: 1.5 }}>{desc}</div>
              </div>
            ))}
          </div>
        </div>
      </Panel>
    </div>
  );
}

// ─── SIDEBAR NAV ─────────────────────────────────────────────────────────────
const NAV = [
  { id: "dashboard", label: "Overview", dot: C.green },
  { id: "anomaly", label: "Anomaly Details", dot: C.red },
  { id: "forecast", label: "Forecasting", dot: C.amber },
  { id: "architecture", label: "Architecture", dot: C.purple },
];

// ─── MAIN APP ────────────────────────────────────────────────────────────────
export default function App() {
  const [page, setPage] = useState("landing");
  const [alerts, setAlerts] = useState(INITIAL_ALERTS);
  const [metrics, setMetrics] = useState({ totalKw: 94312, activeMeters: 98741, alertsHour: 7 });
  const [consumption, setConsumption] = useState(CONSUMPTION);
  const tickRef = useRef(0);

  useEffect(() => {
    if (page === "landing") return;
    const interval = setInterval(() => {
      tickRef.current++;
      setMetrics(m => ({
        totalKw: m.totalKw + Math.floor((Math.random() - 0.5) * 400),
        activeMeters: 98741 + Math.floor(Math.random() * 20 - 10),
        alertsHour: Math.max(1, m.alertsHour + (Math.random() > 0.8 ? 1 : Math.random() > 0.7 ? -1 : 0)),
      }));
      if (tickRef.current % 5 === 0) {
        setAlerts(prev => {
          const newAlert = genAlerts(1)[0];
          newAlert.id = `ALT-${1100 + tickRef.current}`;
          newAlert.minsAgo = 0;
          newAlert.time = new Date().toLocaleTimeString("en", { hour: "2-digit", minute: "2-digit" });
          return [newAlert, ...prev.slice(0, 11)];
        });
      }
    }, 2000);
    return () => clearInterval(interval);
  }, [page]);

  if (page === "landing") {
    return (
      <>
        <style>{css}</style>
        <Landing onEnter={() => setPage("dashboard")} />
      </>
    );
  }

  const titles = { dashboard: "Operations Overview", anomaly: "Anomaly Details", forecast: "Demand Forecasting", architecture: "Data Pipeline" };

  return (
    <>
      <style>{css}</style>
      <div className="sidebar">
        <div className="sidebar-logo">
          <svg className="logo-icon" viewBox="0 0 32 32" fill="none">
            <rect width="32" height="32" rx="6" fill="rgba(0,212,255,0.1)" />
            <polyline points="4,22 10,14 16,18 22,8 28,12" stroke="#00d4ff" strokeWidth="2" fill="none" strokeLinecap="round" strokeLinejoin="round" />
            <circle cx="10" cy="14" r="2" fill="#00d4ff" />
            <circle cx="22" cy="8" r="2" fill="#ff3d57" />
          </svg>
          <div className="logo-text">SmartGrid</div>
          <div className="logo-sub">Energy Analytics v2.1</div>
        </div>
        <nav className="nav-section">
          <div className="nav-label">Navigation</div>
          {NAV.map(n => (
            <div key={n.id} className={`nav-item${page === n.id ? " active" : ""}`} onClick={() => setPage(n.id)}>
              <span className="nav-dot" style={{ background: page === n.id ? n.dot : C.border }} />
              {n.label}
              {n.id === "anomaly" && alerts.filter(a => a.status === "Open").length > 0 && (
                <span style={{ marginLeft: "auto", background: "rgba(255,61,87,0.15)", color: C.red, fontSize: 10, fontWeight: 700, padding: "1px 6px", borderRadius: 8, fontFamily: "JetBrains Mono" }}>
                  {alerts.filter(a => a.status === "Open").length}
                </span>
              )}
            </div>
          ))}
          <div className="nav-label" style={{ marginTop: 12 }}>System</div>
          <div className="nav-item" onClick={() => setPage("landing")}>
            <span className="nav-dot" style={{ background: C.border }} />
            Landing Page
          </div>
        </nav>
        <div className="sidebar-footer">
          <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 4 }}>
            <PulseOrb size={5} />
            <span style={{ color: C.green, fontSize: 10, fontWeight: 700 }}>ALL SYSTEMS OPERATIONAL</span>
          </div>
          <div>Cluster A · B · C online</div>
        </div>
      </div>

      <main className="main">
        <div className="topbar">
          <div className="topbar-title">{titles[page]}</div>
          <div className="topbar-right">
            <span style={{ fontSize: 11, color: C.textMuted, fontFamily: "JetBrains Mono" }}>
              {new Date().toLocaleTimeString()}
            </span>
            <div className="status-pill live">
              <PulseOrb size={6} />
              LIVE
            </div>
            <div style={{ width: 30, height: 30, borderRadius: "50%", background: `${C.accent}20`, border: `1px solid ${C.border}`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 12, color: C.accent, fontWeight: 700 }}>
              OP
            </div>
          </div>
        </div>

        {page === "dashboard" && <Dashboard alerts={alerts} consumption={consumption} metrics={metrics} />}
        {page === "anomaly" && <AnomalyPage alerts={alerts} />}
        {page === "forecast" && <ForecastPage />}
        {page === "architecture" && <ArchitecturePage />}
      </main>
    </>
  );
}
