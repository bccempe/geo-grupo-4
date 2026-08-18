import React, { useState, useEffect } from 'react';
import { MapContainer, TileLayer, Marker, Popup, useMapEvents, useMap } from 'react-leaflet';
import L from 'leaflet';
import { Search, MapPin, Loader2, Compass } from 'lucide-react';
import { geocodeAutocomplete, reverseGeocode } from '../api/apiService';

// Fix default Leaflet icon marker URLs
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
});

const redMarkerIcon = new L.Icon({
  iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-red.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  shadowSize: [41, 41]
});

const originEmojiIcon = L.divIcon({
  className: 'emoji-marker',
  html: '<span class="emoji-marker-glyph" role="img" aria-label="Origen">📍</span>',
  iconSize: [28, 28],
  iconAnchor: [14, 24],
  popupAnchor: [0, -20]
});

function MapEventsHandler({ onLocationSelect }) {
  useMapEvents({
    click(e) {
      onLocationSelect(e.latlng.lat, e.latlng.lng);
    },
  });
  return null;
}

function ChangeView({ center }) {
  const map = useMap();
  useEffect(() => {
    map.setView(center, map.getZoom());
  }, [center, map]);
  return null;
}

export default function LocationPicker({ location, setLocation }) {
  const [searchQuery, setSearchQuery] = useState('');
  const [suggestions, setSuggestions] = useState([]);
  const [isSearching, setIsSearching] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);

  const handleSearch = async (queryText) => {
    const q = queryText || searchQuery;
    if (!q || q.trim().length < 3) return;
    setIsSearching(true);
    try {
      const results = await geocodeAutocomplete(q, 5);
      setSuggestions(results);
      setShowDropdown(true);
    } catch (err) {
      console.error('Error autocompletando dirección:', err);
    } finally {
      setIsSearching(false);
    }
  };

  const handleSelectSuggestion = (item) => {
    setLocation({
      lat: item.lat,
      lon: item.lon,
      address: item.display_name,
    });
    setSearchQuery(item.display_name);
    setShowDropdown(false);
  };

  const handleMapClick = async (lat, lon) => {
    setIsSearching(true);
    try {
      const res = await reverseGeocode(lat, lon);
      const addr = res?.short_address || res?.display_name || `${lat.toFixed(5)}, ${lon.toFixed(5)}`;
      setLocation({ lat, lon, address: addr });
    } catch (err) {
      setLocation({ lat, lon, address: `${lat.toFixed(5)}, ${lon.toFixed(5)}` });
    } finally {
      setIsSearching(false);
    }
  };

  return (
    <div className="space-y-3">
      {/* Search Input with Autocomplete */}
      <div className="relative">
        <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1.5 flex items-center gap-1.5">
          <Search className="w-3.5 h-3.5 text-sky-400" />
          Buscar dirección (Autocompletado)
        </label>
        <div className="flex gap-2">
          <div className="relative flex-1">
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => {
                setSearchQuery(e.target.value);
                if (e.target.value.length >= 3) handleSearch(e.target.value);
              }}
              onFocus={() => suggestions.length > 0 && setShowDropdown(true)}
              placeholder="Ej: Alameda 1050, Santiago"
              className="w-full bg-slate-900 border border-slate-700/80 focus:border-sky-500 focus:ring-1 focus:ring-sky-500 text-slate-100 placeholder-slate-500 text-sm rounded-xl px-3.5 py-2.5 outline-none transition-all"
            />
            {isSearching && (
              <div className="absolute right-3 top-3 text-sky-400">
                <Loader2 className="w-4 h-4 animate-spin" />
              </div>
            )}
          </div>
          <button
            onClick={() => handleSearch()}
            className="px-4 py-2.5 bg-sky-600 hover:bg-sky-500 text-white rounded-xl text-sm font-medium transition-all shadow-md shadow-sky-600/20 flex items-center gap-1.5"
          >
            <Search className="w-4 h-4" />
            <span>Buscar</span>
          </button>
        </div>

        {/* Autocomplete Dropdown */}
        {showDropdown && suggestions.length > 0 && (
          <div className="absolute left-0 right-0 top-full mt-1 bg-slate-900 border border-slate-700 rounded-xl shadow-2xl z-50 overflow-hidden divide-y divide-slate-800">
            {suggestions.map((item, idx) => (
              <div
                key={idx}
                onClick={() => handleSelectSuggestion(item)}
                className="px-4 py-2.5 hover:bg-slate-800 cursor-pointer text-xs text-slate-200 flex items-center space-x-2 transition-colors"
              >
                <MapPin className="w-4 h-4 text-sky-400 shrink-0" />
                <span className="truncate">{item.display_name}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Map Selector */}
      <div className="h-64 rounded-xl overflow-hidden border border-slate-800 relative shadow-inner">
        <MapContainer
          center={[location.lat, location.lon]}
          zoom={14}
          scrollWheelZoom={true}
          style={{ height: '100%', width: '100%' }}
        >
          <ChangeView center={[location.lat, location.lon]} />
          <TileLayer
            attribution='&copy; <a href="https://carto.com/">CARTO</a>'
            url="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png"
          />
          <Marker position={[location.lat, location.lon]} icon={originEmojiIcon}>
            <Popup>{location.address || 'Punto de Origen'}</Popup>
          </Marker>
          <MapEventsHandler onLocationSelect={handleMapClick} />
        </MapContainer>
      </div>

      {/* Selected Address Display */}
      <div className="p-3 bg-slate-900/60 border border-slate-800 rounded-xl flex items-start gap-2 text-xs text-slate-300">
        <Compass className="w-4 h-4 text-sky-400 shrink-0 mt-0.5" />
        <div>
          <span className="font-semibold text-slate-200">Ubicación seleccionada:</span>
          <p className="text-slate-400 mt-0.5">{location.address || `${location.lat.toFixed(5)}, ${location.lon.toFixed(5)}`}</p>
        </div>
      </div>
    </div>
  );
}
