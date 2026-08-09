import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || '';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 1800000, // 30 minutes for heavy RM calculations
});

export const comunasDisponibles = [
  "Alhué", "Buin", "Calera de Tango", "Cerrillos", "Cerro Navia", "Colina", "Conchalí",
  "Curacaví", "El Bosque", "El Monte", "Estación Central", "Huechuraba", "Independencia",
  "Isla de Maipo", "La Cisterna", "La Florida", "La Granja", "La Pintana", "La Reina",
  "Lampa", "Las Condes", "Lo Barnechea", "Lo Espejo", "Lo Prado", "Macul", "Maipú",
  "María Pinto", "Melipilla", "Ñuñoa", "Padre Hurtado", "Paine", "Pedro Aguirre Cerda",
  "Peñaflor", "Peñalolén", "Pirque", "Providencia", "Pudahuel", "Puente Alto",
  "Quilicura", "Quinta Normal", "Recoleta", "Renca", "San Bernardo", "San Joaquín",
  "San José de Maipo", "San Miguel", "San Pedro", "San Ramón", "Santiago", "Talagante", "Vitacura"
].sort();

export function removeAccents(text) {
  if (!text) return "";
  return text.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

export function normalizeComuna(text) {
  if (!text) return "";
  let norm = removeAccents(text).trim().toLowerCase();
  norm = norm.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return norm;
}

export const fetchIsochrone = async (type, lat, lon, minutes, comuna = null) => {
  const endpoint = type === 'transit' ? '/api/v1/transit/isochrone' : '/api/v1/isochrone';
  const params = { lat, lon, minutes, include_centers: true };
  if (comuna) params.comuna = comuna;
  const res = await api.get(endpoint, { params });
  return res.data;
};

export const fetchHealthDeserts = async (type, comuna, minutes) => {
  const endpoint = type === 'transit' ? '/api/v1/transit/health-deserts' : '/api/v1/health-deserts';
  const params = { comuna: normalizeComuna(comuna), minutes };
  const res = await api.get(endpoint, { params });
  return res.data;
};

export const fetchPopulationCoverage = async (comuna, minutes, mode = 'walk', departureHour = null) => {
  const endpoint = mode === 'transit'
    ? '/api/v1/population/transit-coverage'
    : '/api/v1/population/coverage';
  const params = { comuna: normalizeComuna(comuna), minutes };
  if (mode === 'transit' && departureHour !== null) {
    params.departure_hour = departureHour;
  }
  const res = await api.get(endpoint, { params });
  return res.data;
};

export const geocodeAutocomplete = async (query, limit = 5) => {
  if (!query || query.trim().length < 3) return [];
  const res = await api.get('/api/v1/geocode/autocomplete', { params: { q: query, limit } });
  return res.data?.results || [];
};

export const reverseGeocode = async (lat, lon) => {
  const res = await api.get('/api/v1/geocode/reverse', { params: { lat, lon } });
  return res.data;
};

export const exportHealthDesertPNG = async (data, minutes, comuna, mode) => {
  const response = await api.post('/api/v1/export/health-desert', {
    data,
    minutes,
    comuna,
    mode
  }, { responseType: 'blob' });
  
  const url = window.URL.createObjectURL(new Blob([response.data]));
  const link = document.createElement('a');
  link.href = url;
  link.setAttribute('download', `desierto_salud_${normalizeComuna(comuna)}_${minutes}min.png`);
  document.body.appendChild(link);
  link.click();
  link.remove();
};

export const exportPopulationCoveragePNG = async (data, minutes, comuna) => {
  const response = await api.post('/api/v1/export/population-coverage', {
    data,
    minutes,
    comuna
  }, { responseType: 'blob' });
  
  const url = window.URL.createObjectURL(new Blob([response.data]));
  const link = document.createElement('a');
  link.href = url;
  link.setAttribute('download', `cobertura_poblacional_${normalizeComuna(comuna)}_${minutes}min.png`);
  document.body.appendChild(link);
  link.click();
  link.remove();
};
