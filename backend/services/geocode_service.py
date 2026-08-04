import time
import threading
import requests
from typing import List, Dict, Any, Optional

class GeocodeService:
    """
    Servicio para autocompletar direcciones y realizar geocodificación
    directa (dirección -> coordenadas) e inversa (coordenadas -> dirección)
    utilizando Nominatim (OpenStreetMap) con limitación estricta de 1 req/seg.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(GeocodeService, cls).__new__(cls)
                    cls._instance._init_service()
        return cls._instance

    def _init_service(self):
        self.min_interval = 1.0  # Máximo 1 solicitud por segundo a Nominatim
        self.last_request_time = 0.0
        self.rate_lock = threading.Lock()
        self.headers = {
            "User-Agent": "GeoSaludRM/1.0 (contacto-salud-rm@usach.cl)"
        }
        self.search_cache: Dict[str, List[Dict[str, Any]]] = {}
        self.reverse_cache: Dict[str, Dict[str, Any]] = {}

    def _wait_rate_limit(self):
        with self.rate_lock:
            now = time.time()
            elapsed = now - self.last_request_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_request_time = time.time()

    def search_address(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Busca direcciones que coincidan con la consulta dentro de Chile / RM.
        """
        if not query or len(query.strip()) < 3:
            return []

        clean_query = query.strip().lower()
        cache_key = f"{clean_query}_{limit}"

        if cache_key in self.search_cache:
            return self.search_cache[cache_key]

        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": query,
            "format": "jsonv2",
            "addressdetails": 1,
            "countrycodes": "cl",
            "viewbox": "-71.8,-34.2,-69.7,-32.8",
            "bounded": 0,
            "limit": limit
        }

        try:
            self._wait_rate_limit()
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            
            if response.status_code != 200:
                print(f"[GeocodeService] Error HTTP {response.status_code} desde Nominatim")
                return []

            data = response.json()
            results = []

            for item in data:
                address_info = item.get("address", {})
                comuna = (
                    address_info.get("city") or 
                    address_info.get("town") or 
                    address_info.get("suburb") or 
                    address_info.get("county") or 
                    address_info.get("state_district") or 
                    ""
                )
                
                display_name = item.get("display_name", "")
                lat = float(item.get("lat"))
                lon = float(item.get("lon"))

                results.append({
                    "display_name": display_name,
                    "address": address_info,
                    "comuna": comuna,
                    "lat": lat,
                    "lon": lon,
                    "type": item.get("type", ""),
                    "category": item.get("category", "")
                })

            self.search_cache[cache_key] = results
            return results

        except Exception as e:
            print(f"[GeocodeService] Excepción al buscar dirección: {e}")
            return []

    def reverse_geocode(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """
        Convierte (lat, lon) a una dirección formateada.
        """
        cache_key = f"{lat:.5f}_{lon:.5f}"
        if cache_key in self.reverse_cache:
            return self.reverse_cache[cache_key]

        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            "lat": lat,
            "lon": lon,
            "format": "jsonv2",
            "addressdetails": 1
        }

        try:
            self._wait_rate_limit()
            response = requests.get(url, params=params, headers=self.headers, timeout=10)

            if response.status_code != 200:
                print(f"[GeocodeService] Error HTTP {response.status_code} desde Nominatim Reverse")
                return None

            item = response.json()
            address_info = item.get("address", {})
            comuna = (
                address_info.get("city") or 
                address_info.get("town") or 
                address_info.get("suburb") or 
                address_info.get("county") or 
                address_info.get("state_district") or 
                ""
            )

            road = address_info.get("road", "")
            house_number = address_info.get("house_number", "")
            
            if road:
                short_address = f"{road} {house_number}".strip()
                if comuna:
                    short_address += f", {comuna}"
            else:
                short_address = item.get("display_name", f"{lat:.4f}, {lon:.4f}")

            result = {
                "display_name": item.get("display_name", ""),
                "short_address": short_address,
                "address": address_info,
                "comuna": comuna,
                "lat": lat,
                "lon": lon
            }

            self.reverse_cache[cache_key] = result
            return result

        except Exception as e:
            print(f"[GeocodeService] Excepción en reverse geocoding: {e}")
            return None
