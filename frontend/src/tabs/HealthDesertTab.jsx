import React, { useState } from 'react';
import MapView from '../components/MapView';
import StatCard from '../components/StatCard';
import { comunasDisponibles, fetchHealthDeserts, exportHealthDesertPNG } from '../api/apiService';
import { ShieldAlert, Download, Loader2, Hospital, Percent, Footprints, Bus } from 'lucide-react';

export default function HealthDesertTab() {
  const [comuna, setComuna] = useState('Santiago');
  const [transportMode, setTransportMode] = useState('walk');
  const [minutes, setMinutes] = useState(30);
  const [loading, setLoading] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const handleCalculate = async () => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const data = await fetchHealthDeserts(transportMode, comuna, minutes);
      setResult(data);
    } catch (err) {
      console.error('Error calculando desiertos de salud:', err);
      setError(err.response?.data?.detail || 'Error al calcular desiertos de salud.');
    } finally {
      setLoading(false);
    }
  };

  const handleExport = async () => {
    if (!result) return;
    setExporting(true);
    try {
      const modeLabel = transportMode === 'walk' ? 'caminando' : 'transporte publico';
      await exportHealthDesertPNG(result, minutes, comuna, modeLabel);
    } catch (err) {
      alert('Error exportando mapa PNG');
    } finally {
      setExporting(false);
    }
  };

  const meta = result?.metadata || {};
  const modeLabel = transportMode === 'walk' ? 'Caminando' : 'Transporte Público';
  const modeIcon = transportMode === 'walk' ? Footprints : Bus;
  const ModeIcon = modeIcon;

  return (
    <div className="space-y-6">
      {/* Intro Header */}
      <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 backdrop-blur-md">
        <div className="flex items-center space-x-3">
          <div className="p-2.5 bg-rose-500/10 border border-rose-500/30 rounded-xl text-rose-400">
            <ShieldAlert className="w-5 h-5" />
          </div>
          <div>
            <h2 className="text-xl font-bold text-white font-['Outfit']">Desiertos de Salud por Comuna</h2>
            <p className="text-xs text-slate-400 mt-0.5">
              Calcula las zonas urbanas sin acceso oportuno a centros de salud primaria (CESFAM/SAPU).
            </p>
          </div>
        </div>
      </div>

      {/* Control Bar */}
      <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 backdrop-blur-md grid grid-cols-1 md:grid-cols-4 gap-4 items-end">
        {/* Transport Mode */}
        <div className="space-y-1.5">
          <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">Modo</label>
          <div className="flex bg-slate-950 p-1 rounded-xl border border-slate-800">
            <button
              onClick={() => { setTransportMode('walk'); setResult(null); }}
              className={`flex-1 py-1.5 rounded-lg text-xs font-medium transition-all flex items-center justify-center gap-1 ${
                transportMode === 'walk' ? 'bg-rose-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Footprints className="w-3.5 h-3.5" />
              Caminata
            </button>
            <button
              onClick={() => { setTransportMode('transit'); setResult(null); }}
              className={`flex-1 py-1.5 rounded-lg text-xs font-medium transition-all flex items-center justify-center gap-1 ${
                transportMode === 'transit' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Bus className="w-3.5 h-3.5" />
              TP
            </button>
          </div>
        </div>

        {/* Comuna Selector */}
        <div className="space-y-1.5">
          <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">
            Comuna a calcular
          </label>
          <select
            value={comuna}
            onChange={(e) => setComuna(e.target.value)}
            className="w-full bg-slate-950 border border-slate-700/80 focus:border-rose-500 text-slate-100 text-sm rounded-xl px-3.5 py-2.5 outline-none"
          >
            {comunasDisponibles.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </div>

        {/* Minutes Slider */}
        <div className="space-y-1.5">
          <div className="flex justify-between items-center text-xs text-slate-300">
            <label className="font-semibold uppercase tracking-wider text-slate-400">Minutos estimados:</label>
            <span className="px-2.5 py-0.5 bg-rose-500/10 border border-rose-500/30 text-rose-400 rounded-lg font-bold">
              {minutes} min
            </span>
          </div>
          <input
            type="range"
            min={15}
            max={60}
            step={5}
            value={minutes}
            onChange={(e) => setMinutes(Number(e.target.value))}
            className="w-full h-2 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-rose-500"
          />
        </div>

        {/* Calculate Button */}
        <button
          onClick={handleCalculate}
          disabled={loading}
          className="w-full py-2.5 bg-gradient-to-r from-rose-500 to-pink-600 hover:from-rose-400 hover:to-pink-500 text-white font-semibold rounded-xl text-sm transition-all shadow-lg shadow-rose-500/25 flex items-center justify-center space-x-2 disabled:opacity-50"
        >
          {loading ? (
            <>
              <Loader2 className="w-4 h-4 animate-spin" />
              <span>Calculando...</span>
            </>
          ) : (
            <>
              <ShieldAlert className="w-4 h-4" />
              <span>Calcular Desiertos</span>
            </>
          )}
        </button>
      </div>

      {error && (
        <div className="p-4 bg-rose-500/10 border border-rose-500/30 text-rose-400 rounded-xl text-sm">
          {error}
        </div>
      )}

      {/* Results View */}
      <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 backdrop-blur-md space-y-4">
        <div className="flex justify-between items-center">
          <div className={`flex items-center space-x-2 font-semibold text-sm ${transportMode === 'walk' ? 'text-rose-400' : 'text-indigo-400'}`}>
            <ModeIcon className="w-4 h-4" />
            <span>{modeLabel} — {minutes} min ({comuna})</span>
          </div>
          {result && (
            <button
              onClick={handleExport}
              disabled={exporting}
              className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-medium rounded-lg border border-slate-700 flex items-center gap-1.5 transition-all"
            >
              {exporting ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Download className="w-3.5 h-3.5 text-sky-400" />}
              <span>Exportar PNG</span>
            </button>
          )}
        </div>

        {result ? (
          <>
            <div className="grid grid-cols-2 gap-3">
              <StatCard
                title="Centros en Comuna"
                value={meta.centers_count ?? 0}
                icon={Hospital}
                color="emerald"
              />
              <StatCard
                title="Nivel Desierto"
                value={`${(meta.desert_pct ?? 0).toFixed(1)}%`}
                icon={Percent}
                color="rose"
              />
            </div>
            <MapView geoJsonData={result} type="desert" minutes={minutes} />
          </>
        ) : (
          <div className="h-64 border border-dashed border-slate-800 rounded-xl flex items-center justify-center text-slate-500 text-xs">
            Selecciona comuna, modo y haz clic en "Calcular Desiertos".
          </div>
        )}
      </div>
    </div>
  );
}
