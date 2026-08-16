import React, { useState } from 'react';
import MapView from '../components/MapView';
import StatCard from '../components/StatCard';
import {
  comunasDisponibles,
  exportPopulationCoveragePNG,
  fetchPopulationCoverage,
} from '../api/apiService';
import {
  Bus,
  CarFront,
  Download,
  Footprints,
  HeartHandshake,
  Layers,
  Loader2,
  Percent,
  UserCheck,
  Users,
} from 'lucide-react';

const TRAVEL_MODES = [
  { id: 'foot', label: 'Caminando', icon: Footprints },
  { id: 'car', label: 'Automóvil', icon: CarFront },
  { id: 'transit', label: 'Transporte público', icon: Bus },
];

function calculateStats(data) {
  if (!data?.features) return null;

  const totals = data.features.reduce((result, feature) => {
    if (feature.properties?.kind !== 'census_block') return result;
    return {
      totalPopulation: result.totalPopulation + (feature.properties.population || 0),
      coveredPopulation: result.coveredPopulation + (feature.properties.covered_population || 0),
      elderlyPopulation: result.elderlyPopulation + (feature.properties.elderly_population || 0),
      coveredElderly: result.coveredElderly + (feature.properties.covered_elderly_population || 0),
    };
  }, {
    totalPopulation: 0,
    coveredPopulation: 0,
    elderlyPopulation: 0,
    coveredElderly: 0,
  });

  return {
    ...totals,
    coveragePct: totals.totalPopulation > 0
      ? (totals.coveredPopulation / totals.totalPopulation) * 100
      : 0,
  };
}

function requestErrorMessage(error) {
  return error.response?.data?.detail || error.message || 'Error al calcular cobertura.';
}

export default function CoverageTab() {
  const [scope, setScope] = useState('comuna');
  const [travelMode, setTravelMode] = useState('foot');
  const [comuna, setComuna] = useState('Santiago');
  const [minutes, setMinutes] = useState(15);
  const [departureHour, setDepartureHour] = useState(8);
  const [loading, setLoading] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [coverageResult, setCoverageResult] = useState(null);
  const [error, setError] = useState(null);

  const activeTravelMode = TRAVEL_MODES.find((item) => item.id === travelMode);
  const stats = calculateStats(coverageResult);

  const resetResult = () => {
    setCoverageResult(null);
    setError(null);
  };

  const handleCalculate = async () => {
    setLoading(true);
    setError(null);
    setCoverageResult(null);
    try {
      const data = await fetchPopulationCoverage(
        comuna,
        minutes,
        travelMode,
        scope,
        travelMode === 'transit' ? departureHour : null,
      );
      setCoverageResult(data);
    } catch (requestError) {
      console.error('Error calculando cobertura poblacional:', requestError);
      setError(requestErrorMessage(requestError));
    } finally {
      setLoading(false);
    }
  };

  const handleExportPNG = async () => {
    if (!coverageResult) return;
    setExporting(true);
    try {
      const areaLabel = scope === 'rm' ? 'Región Metropolitana' : comuna;
      await exportPopulationCoveragePNG(
        coverageResult,
        minutes,
        areaLabel,
        travelMode,
      );
    } catch (exportError) {
      console.error('Error exportando cobertura poblacional:', exportError);
      window.alert(exportError.message || 'Error exportando mapa PNG');
    } finally {
      setExporting(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="bg-slate-900/60 border border-slate-800 rounded-lg p-5 backdrop-blur-md">
        <div className="flex items-center space-x-3">
          <div className="p-2.5 bg-blue-500/10 border border-blue-500/30 rounded-lg text-blue-400">
            <Users className="w-5 h-5" />
          </div>
          <div>
            <h2 className="text-xl font-bold text-white font-['Outfit']">Cobertura Poblacional</h2>
            <p className="text-xs text-slate-400 mt-0.5">
              Población cubierta por centros de salud primaria según manzana censal y modo de viaje.
            </p>
          </div>
        </div>
      </div>

      <div className="bg-slate-900/60 border border-slate-800 rounded-lg p-5 backdrop-blur-md space-y-5">
        <div className="space-y-1.5">
          <label className="block text-xs font-semibold uppercase text-slate-400">Modo de viaje</label>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800">
            {TRAVEL_MODES.map(({ id, label, icon: Icon }) => (
              <button
                key={id}
                type="button"
                onClick={() => {
                  setTravelMode(id);
                  resetResult();
                }}
                className={`min-h-10 px-3 py-2 rounded-md text-xs font-medium transition-colors flex items-center justify-center gap-2 ${
                  travelMode === id
                    ? 'bg-blue-600 text-white shadow'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-900'
                }`}
              >
                <Icon className="w-4 h-4" />
                <span>{label}</span>
              </button>
            ))}
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 items-end">
          <div className="space-y-1.5">
            <label className="block text-xs font-semibold uppercase text-slate-400">Alcance</label>
            <div className="flex bg-slate-950 p-1 rounded-lg border border-slate-800">
              <button
                type="button"
                onClick={() => {
                  setScope('comuna');
                  resetResult();
                }}
                className={`flex-1 min-h-9 rounded-md text-xs font-medium ${
                  scope === 'comuna' ? 'bg-blue-600 text-white' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                Comuna
              </button>
              <button
                type="button"
                onClick={() => {
                  setScope('rm');
                  resetResult();
                }}
                className={`flex-1 min-h-9 rounded-md text-xs font-medium ${
                  scope === 'rm' ? 'bg-blue-600 text-white' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                RM completa
              </button>
            </div>
          </div>

          <div className="space-y-1.5">
            <label className="block text-xs font-semibold uppercase text-slate-400">Comuna</label>
            <select
              value={comuna}
              onChange={(event) => {
                setComuna(event.target.value);
                resetResult();
              }}
              disabled={scope === 'rm'}
              className="w-full min-h-11 bg-slate-950 border border-slate-700 focus:border-blue-500 disabled:opacity-50 text-slate-100 text-sm rounded-lg px-3 outline-none"
            >
              {comunasDisponibles.map((item) => (
                <option key={item} value={item}>{item}</option>
              ))}
            </select>
          </div>

          <div className="space-y-1.5">
            <div className="flex justify-between items-center text-xs">
              <label className="font-semibold uppercase text-slate-400">Tiempo de viaje</label>
              <span className="px-2 py-1 bg-blue-500/10 border border-blue-500/30 text-blue-400 rounded-md font-bold">
                {minutes} min
              </span>
            </div>
            <input
              type="range"
              min={5}
              max={60}
              step={5}
              value={minutes}
              onChange={(event) => {
                setMinutes(Number(event.target.value));
                resetResult();
              }}
              className="w-full h-2 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-blue-500"
            />
          </div>

          <div className="space-y-1.5">
            <label className="block text-xs font-semibold uppercase text-slate-400">Hora de salida</label>
            <select
              value={departureHour}
              onChange={(event) => {
                setDepartureHour(Number(event.target.value));
                resetResult();
              }}
              disabled={travelMode !== 'transit'}
              className="w-full min-h-11 bg-slate-950 border border-slate-700 focus:border-blue-500 disabled:opacity-50 text-slate-100 text-sm rounded-lg px-3 outline-none"
            >
              {Array.from({ length: 24 }, (_, hour) => (
                <option key={hour} value={hour}>{String(hour).padStart(2, '0')}:00</option>
              ))}
            </select>
          </div>
        </div>

        <button
          type="button"
          onClick={handleCalculate}
          disabled={loading}
          className="w-full min-h-12 bg-blue-600 hover:bg-blue-500 text-white font-semibold rounded-lg text-sm transition-colors flex items-center justify-center gap-2 disabled:opacity-50"
        >
          {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Layers className="w-4 h-4" />}
          <span>{loading ? 'Calculando cobertura...' : `Calcular cobertura ${activeTravelMode.label.toLowerCase()}`}</span>
        </button>
      </div>

      {error && (
        <div className="p-4 bg-rose-500/10 border border-rose-500/30 text-rose-400 rounded-lg text-sm">
          {error}
        </div>
      )}

      {coverageResult && stats && (
        <div className="space-y-6">
          <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
            <StatCard title="Población Total" value={Math.round(stats.totalPopulation).toLocaleString()} icon={Users} color="sky" />
            <StatCard title="Población Cubierta" value={Math.round(stats.coveredPopulation).toLocaleString()} icon={UserCheck} color="emerald" />
            <StatCard title="Adultos Mayores" value={Math.round(stats.elderlyPopulation).toLocaleString()} icon={HeartHandshake} color="amber" />
            <StatCard title="Mayores Cubiertos" value={Math.round(stats.coveredElderly).toLocaleString()} icon={UserCheck} color="indigo" />
            <StatCard title="Porcentaje Cobertura" value={`${stats.coveragePct.toFixed(1)}%`} icon={Percent} color="emerald" />
          </div>

          <div className="bg-slate-900/60 border border-slate-800 rounded-lg p-5 backdrop-blur-md space-y-4">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
              <div className="flex items-center gap-2 text-blue-400 font-semibold text-sm">
                <Layers className="w-4 h-4" />
                <span>
                  {activeTravelMode.label} · {scope === 'rm' ? 'Región Metropolitana' : comuna}
                </span>
              </div>
              <button
                type="button"
                onClick={handleExportPNG}
                disabled={exporting}
                className="min-h-10 px-4 bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold rounded-lg flex items-center justify-center gap-2 transition-colors disabled:opacity-50"
              >
                {exporting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Download className="w-4 h-4" />}
                <span>Exportar PNG</span>
              </button>
            </div>

            <MapView
              key={`${travelMode}-${scope}-${comuna}-${minutes}-${departureHour}`}
              geoJsonData={coverageResult}
              type="coverage"
              minutes={minutes}
            />
          </div>
        </div>
      )}
    </div>
  );
}
