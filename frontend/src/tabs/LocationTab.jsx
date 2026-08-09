import React, { useState } from 'react';
import MapView from '../components/MapView';
import StatCard from '../components/StatCard';
import { comunasDisponibles, fetchLocationOptimization } from '../api/apiService';
import { MapPinPlus, Loader2, Hospital, Users, HeartHandshake, Target } from 'lucide-react';

export default function LocationTab() {
  const [comuna, setComuna] = useState('Santiago');
  const [minutes, setMinutes] = useState(30);
  const [maxCenters, setMaxCenters] = useState(3);
  const [prioritizeElderly, setPrioritizeElderly] = useState(true);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const handleCalculate = async () => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const data = await fetchLocationOptimization(comuna, minutes, maxCenters, prioritizeElderly);
      setResult(data);
    } catch (err) {
      console.error('Error calculando ubicaciones optimas:', err);
      setError(err.response?.data?.detail || 'Error al calcular ubicaciones optimas.');
    } finally {
      setLoading(false);
    }
  };

  const meta = result?.metadata || {};
  const proposals = result?.features?.filter(f => f.properties?.kind === 'proposed_center') || [];

  return (
    <div className="space-y-6">
      {/* Intro Header */}
      <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 backdrop-blur-md">
        <div className="flex items-center space-x-3">
          <div className="p-2.5 bg-amber-500/10 border border-amber-500/30 rounded-xl text-amber-400">
            <Target className="w-5 h-5" />
          </div>
          <div>
            <h2 className="text-xl font-bold text-white font-['Outfit']">Ubicaciones Optimas para Nuevos Centros</h2>
            <p className="text-xs text-slate-400 mt-0.5">
              Sugiere donde construir nuevos CESFAM/SAPU para maximizar la cobertura de poblacion no atendida.
            </p>
          </div>
        </div>
      </div>

      {/* Control Bar */}
      <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 backdrop-blur-md grid grid-cols-1 md:grid-cols-5 gap-4 items-end">
        {/* Comuna Selector */}
        <div className="space-y-1.5">
          <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">Comuna</label>
          <select
            value={comuna}
            onChange={(e) => setComuna(e.target.value)}
            className="w-full bg-slate-950 border border-slate-700/80 focus:border-amber-500 text-slate-100 text-sm rounded-xl px-3.5 py-2.5 outline-none"
          >
            {comunasDisponibles.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </div>

        {/* Minutes Slider */}
        <div className="space-y-1.5">
          <div className="flex justify-between items-center text-xs text-slate-300">
            <label className="font-semibold uppercase tracking-wider text-slate-400">Tiempo max:</label>
            <span className="px-2.5 py-0.5 bg-amber-500/10 border border-amber-500/30 text-amber-400 rounded-lg font-bold">
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
            className="w-full h-2 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-amber-500"
          />
        </div>

        {/* Max Centers */}
        <div className="space-y-1.5">
          <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">Max. centros</label>
          <select
            value={maxCenters}
            onChange={(e) => setMaxCenters(Number(e.target.value))}
            className="w-full bg-slate-950 border border-slate-700/80 focus:border-amber-500 text-slate-100 text-sm rounded-xl px-3.5 py-2.5 outline-none"
          >
            {[1, 2, 3, 4, 5].map((n) => (
              <option key={n} value={n}>{n} centro{n > 1 ? 's' : ''}</option>
            ))}
          </select>
        </div>

        {/* Priorize Elderly */}
        <div className="space-y-1.5 flex flex-col">
          <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400">Priorizar</label>
          <div className="flex bg-slate-950 p-1 rounded-xl border border-slate-800 h-10">
            <button
              onClick={() => setPrioritizeElderly(true)}
              className={`flex-1 py-1.5 rounded-lg text-xs font-medium transition-all flex items-center justify-center gap-1 ${
                prioritizeElderly ? 'bg-amber-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <HeartHandshake className="w-3.5 h-3.5" />
              AM
            </button>
            <button
              onClick={() => setPrioritizeElderly(false)}
              className={`flex-1 py-1.5 rounded-lg text-xs font-medium transition-all flex items-center justify-center gap-1 ${
                !prioritizeElderly ? 'bg-amber-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Users className="w-3.5 h-3.5" />
              Total
            </button>
          </div>
        </div>

        {/* Calculate Button */}
        <button
          onClick={handleCalculate}
          disabled={loading}
          className="w-full py-2.5 bg-gradient-to-r from-amber-500 to-orange-600 hover:from-amber-400 hover:to-orange-500 text-white font-semibold rounded-xl text-sm transition-all shadow-lg shadow-amber-500/25 flex items-center justify-center space-x-2 disabled:opacity-50"
        >
          {loading ? (
            <>
              <Loader2 className="w-4 h-4 animate-spin" />
              <span>Calculando...</span>
            </>
          ) : (
            <>
              <MapPinPlus className="w-4 h-4" />
              <span>Optimizar Ubicaciones</span>
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
          <div className="flex items-center space-x-2 font-semibold text-sm text-amber-400">
            <Target className="w-4 h-4" />
            <span>{comuna} — {minutes} min — {maxCenters} centro{maxCenters > 1 ? 's' : ''} max</span>
          </div>
          <div className="text-xs text-slate-500">
            {meta.existing_centers_count != null && (
              <span>{meta.existing_centers_count} centros existentes</span>
            )}
          </div>
        </div>

        {result ? (
          <>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <StatCard
                title="Propuestas"
                value={meta.proposals_count ?? 0}
                icon={MapPinPlus}
                color="amber"
              />
              <StatCard
                title="Centros existentes"
                value={meta.existing_centers_count ?? 0}
                icon={Hospital}
                color="emerald"
              />
              <StatCard
                title="Sin cobertura restante"
                value={meta.remaining_uncovered_population != null
                  ? `${Math.round(meta.remaining_uncovered_population).toLocaleString()}`
                  : '—'}
                subtitle={meta.remaining_uncovered_elderly != null
                  ? `${Math.round(meta.remaining_uncovered_elderly)} AM`
                  : null}
                icon={Users}
                color="rose"
              />
              <StatCard
                title="Prioridad"
                value={prioritizeElderly ? 'Adultos Mayores' : 'Pob. Total'}
                icon={prioritizeElderly ? HeartHandshake : Users}
                color="indigo"
              />
            </div>

            {/* Per-proposal stats */}
            {proposals.length > 0 && (
              <div className="overflow-x-auto">
                <table className="w-full text-xs text-slate-300 border-collapse">
                  <thead>
                    <tr className="border-b border-slate-700 text-slate-400">
                      <th className="text-left py-2 px-3">#</th>
                      <th className="text-left py-2 px-3">Lat</th>
                      <th className="text-left py-2 px-3">Lon</th>
                      <th className="text-left py-2 px-3">Pob. cubierta</th>
                      <th className="text-left py-2 px-3">AM cubiertos</th>
                    </tr>
                  </thead>
                  <tbody>
                    {proposals.map((f, idx) => {
                      const coords = f.geometry?.coordinates || [];
                      return (
                        <tr key={idx} className="border-b border-slate-800/50 hover:bg-slate-800/30">
                          <td className="py-2 px-3 font-bold text-amber-400">{f.properties?.rank || idx + 1}</td>
                          <td className="py-2 px-3 font-mono">{coords[1]?.toFixed(5)}</td>
                          <td className="py-2 px-3 font-mono">{coords[0]?.toFixed(5)}</td>
                          <td className="py-2 px-3">{Math.round(f.properties?.covered_population || 0).toLocaleString()}</td>
                          <td className="py-2 px-3">{Math.round(f.properties?.covered_elderly || 0).toLocaleString()}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}

            <MapView geoJsonData={result} type="location" minutes={minutes} />
          </>
        ) : (
          <div className="h-64 border border-dashed border-slate-800 rounded-xl flex items-center justify-center text-slate-500 text-xs">
            Selecciona comuna, parametros y haz clic en "Optimizar Ubicaciones".
          </div>
        )}
      </div>
    </div>
  );
}
