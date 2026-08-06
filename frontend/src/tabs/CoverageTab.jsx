import React, { useState } from 'react';
import MapView from '../components/MapView';
import StatCard from '../components/StatCard';
import { comunasDisponibles, fetchPopulationCoverage, exportPopulationCoveragePNG } from '../api/apiService';
import { Users, Download, Loader2, UserCheck, HeartHandshake, Percent, Layers } from 'lucide-react';

export default function CoverageTab() {
  const [mode, setMode] = useState('comuna'); // 'comuna' or 'rm'
  const [comuna, setComuna] = useState('Santiago');
  const [minutes, setMinutes] = useState(15);
  const [loading, setLoading] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [coverageResult, setCoverageResult] = useState(null);
  const [rmProgress, setRmProgress] = useState({ current: 0, total: 0, name: '' });
  const [error, setError] = useState(null);

  const calculateStats = (data) => {
    if (!data || !data.features) return null;
    let totalPop = 0;
    let covPop = 0;
    let eldPop = 0;
    let covEld = 0;

    data.features.forEach((f) => {
      if (f.properties?.kind === 'census_block') {
        totalPop += f.properties.population || 0;
        covPop += f.properties.covered_population || 0;
        eldPop += f.properties.elderly_population || 0;
        covEld += f.properties.covered_elderly_population || 0;
      }
    });

    const pct = totalPop > 0 ? (covPop / totalPop) * 100 : 0;
    return {
      totalPopulation: Math.round(totalPop),
      coveredPopulation: Math.round(covPop),
      elderlyPopulation: Math.round(eldPop),
      coveredElderly: Math.round(covEld),
      coveragePct: pct.toFixed(1)
    };
  };

  const handleCalculateComuna = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchPopulationCoverage(comuna, minutes);
      setCoverageResult(data);
    } catch (err) {
      console.error('Error calculando cobertura poblacional:', err);
      setError(err.response?.data?.detail || 'Error al calcular cobertura.');
    } finally {
      setLoading(false);
    }
  };

  const handleCalculateRM = async () => {
    setLoading(true);
    setError(null);
    setCoverageResult(null);
    const total = comunasDisponibles.length;
    const allFeatures = [];
    const processed = [];

    try {
      for (let i = 0; i < total; i++) {
        const c = comunasDisponibles[i];
        setRmProgress({ current: i + 1, total, name: c });
        try {
          const res = await fetchPopulationCoverage(c, minutes);
          if (res?.features) {
            allFeatures.push(...res.features);
            processed.push(c);
          }
        } catch (e) {
          console.warn(`Omite comuna ${c} por error:`, e);
        }
      }

      setCoverageResult({
        type: 'FeatureCollection',
        features: allFeatures,
        metadata: { scope: 'rm', minutes, processed_count: processed.length }
      });
    } catch (err) {
      setError('Error calculando la Región Metropolitana.');
    } finally {
      setLoading(false);
      setRmProgress({ current: 0, total: 0, name: '' });
    }
  };

  const handleExportPNG = async () => {
    if (!coverageResult) return;
    setExporting(true);
    try {
      const labelComuna = mode === 'rm' ? 'Región Metropolitana' : comuna;
      await exportPopulationCoveragePNG(coverageResult, minutes, labelComuna);
    } catch (err) {
      alert('Error exportando mapa PNG');
    } finally {
      setExporting(false);
    }
  };

  const stats = calculateStats(coverageResult);

  return (
    <div className="space-y-6">
      {/* Intro Header */}
      <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 backdrop-blur-md">
        <div className="flex items-center space-x-3">
          <div className="p-2.5 bg-blue-500/10 border border-blue-500/30 rounded-xl text-blue-400">
            <Users className="w-5 h-5" />
          </div>
          <div>
            <h2 className="text-xl font-bold text-white font-['Outfit']">Cobertura Poblacional</h2>
            <p className="text-xs text-slate-400 mt-0.5">
              Porcentaje de la población que accede a un centro de salud primaria (CESFAM/SAPU) según manzana censal.
            </p>
          </div>
        </div>
      </div>

      {/* Control Bar */}
      <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 backdrop-blur-md space-y-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 items-end">
          {/* Mode Selector */}
          <div className="space-y-1.5">
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">Alcance de Análisis</label>
            <div className="flex bg-slate-950 p-1 rounded-xl border border-slate-800">
              <button
                onClick={() => setMode('comuna')}
                className={`flex-1 py-1.5 rounded-lg text-xs font-medium transition-all ${
                  mode === 'comuna' ? 'bg-blue-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                Por Comuna
              </button>
              <button
                onClick={() => setMode('rm')}
                className={`flex-1 py-1.5 rounded-lg text-xs font-medium transition-all ${
                  mode === 'rm' ? 'bg-blue-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                Región Metropolitana
              </button>
            </div>
          </div>

          {/* Comuna Selector (if mode == 'comuna') */}
          {mode === 'comuna' ? (
            <div className="space-y-1.5">
              <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">Comuna</label>
              <select
                value={comuna}
                onChange={(e) => setComuna(e.target.value)}
                className="w-full bg-slate-950 border border-slate-700/80 focus:border-blue-500 text-slate-100 text-sm rounded-xl px-3.5 py-2.5 outline-none"
              >
                {comunasDisponibles.map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>
          ) : (
            <div className="space-y-1.5 text-xs text-slate-400 bg-slate-950/60 p-2.5 border border-slate-800 rounded-xl">
              <span className="font-semibold text-slate-200">51 Comunas</span> de la Región Metropolitana consolidada.
            </div>
          )}

          {/* Minutes Slider */}
          <div className="space-y-1.5">
            <div className="flex justify-between items-center text-xs text-slate-300">
              <label className="font-semibold uppercase tracking-wider text-slate-400">Minutos vehículo:</label>
              <span className="px-2.5 py-0.5 bg-blue-500/10 border border-blue-500/30 text-blue-400 rounded-lg font-bold">
                {minutes} min
              </span>
            </div>
            <input
              type="range"
              min={5}
              max={60}
              step={5}
              value={minutes}
              onChange={(e) => setMinutes(Number(e.target.value))}
              className="w-full h-2 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-blue-500"
            />
          </div>
        </div>

        {/* Action Button */}
        <button
          onClick={mode === 'comuna' ? handleCalculateComuna : handleCalculateRM}
          disabled={loading}
          className="w-full py-3 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white font-semibold rounded-xl text-sm transition-all shadow-lg shadow-blue-500/25 flex items-center justify-center space-x-2 disabled:opacity-50"
        >
          {loading ? (
            <>
              <Loader2 className="w-4 h-4 animate-spin" />
              <span>
                {rmProgress.total > 0
                  ? `Procesando ${rmProgress.name} (${rmProgress.current}/${rmProgress.total})...`
                  : 'Calculando cobertura...'}
              </span>
            </>
          ) : (
            <>
              <Layers className="w-4 h-4" />
              <span>{mode === 'comuna' ? `Calcular Cobertura (${comuna})` : 'Generar Cobertura Completa RM'}</span>
            </>
          )}
        </button>

        {/* Progress Bar for RM */}
        {loading && rmProgress.total > 0 && (
          <div className="space-y-1.5">
            <div className="w-full bg-slate-950 h-2 rounded-full overflow-hidden border border-slate-800">
              <div
                className="bg-blue-500 h-full transition-all duration-300"
                style={{ width: `${(rmProgress.current / rmProgress.total) * 100}%` }}
              ></div>
            </div>
          </div>
        )}
      </div>

      {error && (
        <div className="p-4 bg-rose-500/10 border border-rose-500/30 text-rose-400 rounded-xl text-sm">
          {error}
        </div>
      )}

      {/* Metrics & Map Display */}
      {coverageResult && (
        <div className="space-y-6">
          {/* Stats Row */}
          {stats && (
            <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
              <StatCard title="Población Total" value={stats.totalPopulation.toLocaleString()} icon={Users} color="sky" />
              <StatCard title="Población Cubierta" value={stats.coveredPopulation.toLocaleString()} icon={UserCheck} color="emerald" />
              <StatCard title="Adultos Mayores" value={stats.elderlyPopulation.toLocaleString()} icon={HeartHandshake} color="amber" />
              <StatCard title="Mayores Cubiertos" value={stats.coveredElderly.toLocaleString()} icon={UserCheck} color="indigo" />
              <StatCard title="Porcentaje Cobertura" value={`${stats.coveragePct}%`} icon={Percent} color="emerald" />
            </div>
          )}

          {/* Map Header & Export PNG Button */}
          <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 backdrop-blur-md space-y-4">
            <div className="flex justify-between items-center">
              <div className="flex items-center space-x-2 text-blue-400 font-semibold text-sm">
                <Layers className="w-4 h-4" />
                <span>Mapa de Cobertura por Manzana ({mode === 'rm' ? 'RM Completa' : comuna})</span>
              </div>
              <button
                onClick={handleExportPNG}
                disabled={exporting}
                className="px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold rounded-xl shadow-md shadow-blue-600/20 flex items-center gap-2 transition-all"
              >
                {exporting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Download className="w-4 h-4" />}
                <span>Exportar Mapa PNG</span>
              </button>
            </div>

            <MapView geoJsonData={coverageResult} type="coverage" minutes={minutes} />
          </div>
        </div>
      )}
    </div>
  );
}
