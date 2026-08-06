import React, { useState } from 'react';
import LocationPicker from '../components/LocationPicker';
import MapView from '../components/MapView';
import { fetchIsochrone } from '../api/apiService';
import { MapPin, Footprints, Bus, Loader2, Navigation } from 'lucide-react';

export default function IsochroneTab() {
  const [location, setLocation] = useState({
    lat: -33.46803,
    lon: -70.67045,
    address: "Av. Libertador Bernardo O'Higgins, Santiago"
  });

  const [minutes, setMinutes] = useState(30);
  const [loading, setLoading] = useState(false);
  const [walkResult, setWalkResult] = useState(null);
  const [transitResult, setTransitResult] = useState(null);
  const [error, setError] = useState(null);

  const handleCalculate = async () => {
    setLoading(true);
    setError(null);
    try {
      const [walkData, transitData] = await Promise.all([
        fetchIsochrone('walk', location.lat, location.lon, minutes),
        fetchIsochrone('transit', location.lat, location.lon, minutes),
      ]);
      setWalkResult(walkData);
      setTransitResult(transitData);
    } catch (err) {
      console.error('Error calculando isócronas:', err);
      setError(err.response?.data?.detail || 'Error al conectar con la API de isócronas.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Intro Header */}
      <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 backdrop-blur-md">
        <div className="flex items-center space-x-3">
          <div className="p-2.5 bg-sky-500/10 border border-sky-500/30 rounded-xl text-sky-400">
            <Navigation className="w-5 h-5" />
          </div>
          <div>
            <h2 className="text-xl font-bold text-white font-['Outfit']">Mapa de Isócronas</h2>
            <p className="text-xs text-slate-400 mt-0.5">
              Busca una dirección o haz clic en el mapa para definir el punto de origen y calcular el alcance en tiempo.
            </p>
          </div>
        </div>
      </div>

      {/* Control Panel & Location Picker */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-1 bg-slate-900/60 border border-slate-800 rounded-2xl p-5 space-y-5 backdrop-blur-md">
          <LocationPicker location={location} setLocation={setLocation} />

          {/* Minutes Slider */}
          <div className="space-y-2">
            <div className="flex justify-between items-center text-xs text-slate-300">
              <label className="font-semibold uppercase tracking-wider text-slate-400">Tiempo de viaje:</label>
              <span className="px-2.5 py-1 bg-sky-500/10 border border-sky-500/30 text-sky-400 rounded-lg font-bold">
                {minutes} min
              </span>
            </div>
            <input
              type="range"
              min={10}
              max={60}
              step={5}
              value={minutes}
              onChange={(e) => setMinutes(Number(e.target.value))}
              className="w-full h-2 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-sky-500"
            />
            <div className="flex justify-between text-[10px] text-slate-500">
              <span>10 min</span>
              <span>30 min</span>
              <span>60 min</span>
            </div>
          </div>

          {/* Calculate Button */}
          <button
            onClick={handleCalculate}
            disabled={loading}
            className="w-full py-3.5 bg-gradient-to-r from-sky-500 to-blue-600 hover:from-sky-400 hover:to-blue-500 text-white font-semibold rounded-xl text-sm transition-all shadow-lg shadow-sky-500/25 flex items-center justify-center space-x-2 disabled:opacity-50"
          >
            {loading ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                <span>Calculando isócronas...</span>
              </>
            ) : (
              <>
                <MapPin className="w-4 h-4" />
                <span>Calcular Isócronas</span>
              </>
            )}
          </button>

          {error && (
            <div className="p-3 bg-rose-500/10 border border-rose-500/30 text-rose-400 rounded-xl text-xs">
              {error}
            </div>
          )}
        </div>

        {/* Results Side */}
        <div className="lg:col-span-2 space-y-6">
          {/* Walking Isochrone */}
          <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 backdrop-blur-md space-y-3">
            <div className="flex items-center space-x-2 text-sky-400 font-semibold text-sm">
              <Footprints className="w-4 h-4" />
              <span>Caminando {minutes} minutos</span>
            </div>
            {walkResult ? (
              <MapView geoJsonData={walkResult} type="isochrone" minutes={minutes} />
            ) : (
              <div className="h-64 border border-dashed border-slate-800 rounded-xl flex items-center justify-center text-slate-500 text-xs">
                Haz clic en "Calcular Isócronas" para generar el mapa caminando.
              </div>
            )}
          </div>

          {/* Transit Isochrone */}
          <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 backdrop-blur-md space-y-3">
            <div className="flex items-center space-x-2 text-indigo-400 font-semibold text-sm">
              <Bus className="w-4 h-4" />
              <span>Usando Transporte Público ({minutes} minutos)</span>
            </div>
            {transitResult ? (
              <MapView geoJsonData={transitResult} type="isochrone" minutes={minutes} />
            ) : (
              <div className="h-64 border border-dashed border-slate-800 rounded-xl flex items-center justify-center text-slate-500 text-xs">
                Haz clic en "Calcular Isócronas" para generar el mapa con transporte público.
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
