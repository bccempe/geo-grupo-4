import React from 'react';
import { MapContainer, TileLayer, GeoJSON, Marker, Popup } from 'react-leaflet';
import L from 'leaflet';

const greenHeartIcon = new L.Icon({
  iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-green.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  shadowSize: [41, 41]
});

const redOriginIcon = new L.Icon({
  iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-red.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  shadowSize: [41, 41]
});

const blueProposedIcon = new L.Icon({
  iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-blue.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  shadowSize: [41, 41]
});

export default function MapView({ geoJsonData, type = 'isochrone', minutes = 30 }) {
  if (!geoJsonData || !geoJsonData.features || geoJsonData.features.length === 0) {
    return (
      <div className="h-[450px] bg-slate-900/40 rounded-2xl border border-slate-800 flex items-center justify-center text-slate-500 text-sm">
        No hay datos para visualizar en el mapa
      </div>
    );
  }

  const features = geoJsonData.features || [];
  let center = [-33.45, -70.65];

  // Extract points (origin & health centers)
  const healthCenters = features.filter(
    (f) => f.properties?.kind === 'health_center' || (f.geometry?.type === 'Point' && f.properties?.kind !== 'origin' && f.properties?.kind !== 'proposed_center')
  );
  const proposedCenters = features.filter(
    (f) => f.properties?.kind === 'proposed_center'
  );
  const originFeature = features.find((f) => f.properties?.kind === 'origin');

  if (originFeature?.geometry?.coordinates) {
    center = [originFeature.geometry.coordinates[1], originFeature.geometry.coordinates[0]];
  }

  // Styling rules
  const getStyle = (feature) => {
    const kind = feature.properties?.kind;
    const ratio = feature.properties?.coverage_ratio ?? 0;

    if (kind === 'isochrone') {
      return {
        fillColor: '#3b82f6',
        color: '#1d4ed8',
        weight: 2,
        fillOpacity: 0.35,
      };
    }
    if (kind === 'coverage') {
      return {
        fillColor: '#10b981',
        color: '#059669',
        weight: 1.5,
        fillOpacity: 0.45,
      };
    }
    if (kind === 'health_desert') {
      return {
        fillColor: '#f43f5e',
        color: '#e11d48',
        weight: 1.5,
        fillOpacity: 0.45,
      };
    }
    if (kind === 'proposed_coverage') {
      return {
        fillColor: '#f59e0b',
        color: '#d97706',
        weight: 2,
        fillOpacity: 0.3,
        dashArray: '5 5',
      };
    }
    if (kind === 'census_block') {
      let fillColor = '#93c5fd';
      if (ratio > 0.75) fillColor = '#1e3a8a';
      else if (ratio > 0.5) fillColor = '#1d4ed8';
      else if (ratio > 0.25) fillColor = '#3b82f6';
      else fillColor = '#93c5fd';

      return {
        fillColor,
        color: '#475569',
        weight: 0.5,
        fillOpacity: 0.7,
      };
    }

    return {
      fillColor: '#64748b',
      color: '#334155',
      weight: 1,
      fillOpacity: 0.4,
    };
  };

  const onEachFeature = (feature, layer) => {
    const props = feature.properties || {};
    if (props.kind === 'census_block') {
      const pop = props.population ? Math.round(props.population) : 0;
      const eld = props.elderly_population ? Math.round(props.elderly_population) : 0;
      const covPct = props.coverage_ratio ? (props.coverage_ratio * 100).toFixed(1) : 0;
      layer.bindTooltip(`
        <div class="text-xs font-sans">
          <b>Población:</b> ${pop}<br/>
          <b>Adultos mayores:</b> ${eld}<br/>
          <b>Cobertura:</b> ${covPct}%
        </div>
      `);
    } else if (props.kind === 'coverage') {
      layer.bindTooltip('Cobertura de salud');
    } else if (props.kind === 'health_desert') {
      layer.bindTooltip('Desierto de salud');
    } else if (props.kind === 'isochrone') {
      layer.bindTooltip(`Isócrona (${minutes} min)`);
    } else if (props.kind === 'proposed_coverage') {
      const rank = props.rank || '';
      layer.bindTooltip(`Cobertura propuesta #${rank}`);
    }
  };

  // Filter polygon features for GeoJSON component
  const polygonFeatures = {
    type: 'FeatureCollection',
    features: features.filter((f) => f.geometry?.type !== 'Point')
  };

  return (
    <div className="h-[480px] rounded-2xl overflow-hidden border border-slate-800 shadow-2xl relative">
      <MapContainer center={center} zoom={12} style={{ height: '100%', width: '100%' }}>
        <TileLayer
          attribution='&copy; <a href="https://carto.com/">CARTO</a>'
          url="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png"
        />

        {polygonFeatures.features.length > 0 && (
          <GeoJSON data={polygonFeatures} style={getStyle} onEachFeature={onEachFeature} />
        )}

        {/* Origin Marker */}
        {originFeature?.geometry?.coordinates && (
          <Marker
            position={[originFeature.geometry.coordinates[1], originFeature.geometry.coordinates[0]]}
            icon={redOriginIcon}
          >
            <Popup>Punto de Origen</Popup>
          </Marker>
        )}

        {/* Health Center Markers */}
        {healthCenters.map((center, idx) => {
          const coords = center.geometry?.coordinates;
          if (!coords || coords.length < 2) return null;
          const name = center.properties?.name || center.properties?.nombre || 'Centro de salud';
          return (
            <Marker key={idx} position={[coords[1], coords[0]]} icon={greenHeartIcon}>
              <Popup>
                <div className="text-xs font-semibold text-slate-800 font-['Inter']">
                  🏥 {name}
                </div>
              </Popup>
            </Marker>
          );
        })}

        {/* Proposed Center Markers */}
        {proposedCenters.map((center, idx) => {
          const coords = center.geometry?.coordinates;
          if (!coords || coords.length < 2) return null;
          const rank = center.properties?.rank || idx + 1;
          const covPop = Math.round(center.properties?.covered_population || 0);
          const covEld = Math.round(center.properties?.covered_elderly || 0);
          return (
            <Marker key={`proposed-${idx}`} position={[coords[1], coords[0]]} icon={blueProposedIcon}>
              <Popup>
                <div className="text-xs font-sans text-slate-800">
                  <b>Centro propuesto #{rank}</b><br/>
                  Pob. cubierta: {covPop.toLocaleString()}<br/>
                  AM cubiertos: {covEld.toLocaleString()}
                </div>
              </Popup>
            </Marker>
          );
        })}
      </MapContainer>

      {/* Floating Legend Overlay */}
      <div className="absolute bottom-4 left-4 bg-slate-900/90 backdrop-blur-md border border-slate-700/80 p-3 rounded-xl shadow-xl z-20 text-xs text-slate-200 space-y-1.5 min-w-[170px]">
        <h4 className="font-semibold text-white border-b border-slate-800 pb-1 mb-1">Simbología</h4>
        {type === 'isochrone' && (
          <>
            <div className="flex items-center gap-2">
              <span className="w-3.5 h-3.5 rounded bg-blue-500/50 border border-blue-600"></span>
              <span>Isócrona ({minutes} min)</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-red-500 font-bold">📍</span>
              <span>Origen</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-emerald-500 font-bold">🏥</span>
              <span>Centro de Salud</span>
            </div>
          </>
        )}
        {type === 'desert' && (
          <>
            <div className="flex items-center gap-2">
              <span className="w-3.5 h-3.5 rounded bg-emerald-500/50 border border-emerald-600"></span>
              <span>Zona Cubierta</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="w-3.5 h-3.5 rounded bg-rose-500/50 border border-rose-600"></span>
              <span>Desierto de Salud</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-emerald-500 font-bold">🏥</span>
              <span>Centro de Salud</span>
            </div>
          </>
        )}
        {type === 'coverage' && (
          <>
            <div className="flex items-center gap-2">
              <span className="w-3.5 h-3.5 rounded bg-blue-900 border border-blue-950"></span>
              <span>Alta (&gt;75%)</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="w-3.5 h-3.5 rounded bg-blue-600 border border-blue-700"></span>
              <span>Media (50-75%)</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="w-3.5 h-3.5 rounded bg-blue-400 border border-blue-500"></span>
              <span>Baja (25-50%)</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="w-3.5 h-3.5 rounded bg-blue-200 border border-blue-300"></span>
              <span>Muy baja (0-25%)</span>
            </div>
          </>
        )}
        {type === 'location' && (
          <>
            <div className="flex items-center gap-2">
              <span className="w-3.5 h-3.5 rounded bg-amber-500/30 border border-amber-600 border-dashed"></span>
              <span>Cobertura propuesta</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-amber-400 font-bold">★</span>
              <span>Centro propuesto</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-emerald-500 font-bold">🏥</span>
              <span>Centro existente</span>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
