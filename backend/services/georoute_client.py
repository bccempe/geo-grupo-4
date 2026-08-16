"""Cliente HTTP para el motor de ruteo georoute."""

import os
from typing import Any

import requests


class GeorouteClient:
    """Consulta un servidor georoute que mantiene el grafo cargado en memoria."""

    def __init__(self, profile: str = "foot"):
        if profile not in {"foot", "car"}:
            raise ValueError("El perfil georoute debe ser 'foot' o 'car'")

        default_url = f"http://georoute-{profile}:8090"
        self.base_url = os.getenv(
            f"GEOROUTE_{profile.upper()}_URL",
            default_url,
        ).rstrip("/")
        self.timeout = float(os.getenv("GEOROUTE_TIMEOUT_SECONDS", "60"))

    def isochrone(self, lon: float, lat: float, minutes: float) -> dict[str, Any]:
        """Obtiene una isócrona como FeatureCollection en EPSG:4326."""
        return self._request(
            "get",
            "/isochrone",
            params={"from": f"{lon},{lat}", "minutes": str(minutes)},
        )

    def access(
        self,
        demand: list[list[float]],
        supply: list[list[float]],
        minutes: float,
        decay: str = "step",
    ) -> list[dict[str, float]]:
        """Calcula accesibilidad 2SFCA para puntos de demanda y oferta."""
        return self._request(
            "post",
            "/access",
            json={
                "demand": demand,
                "supply": supply,
                "minutes": minutes,
                "decay": decay,
            },
        )

    def _request(self, method: str, path: str, **kwargs) -> Any:
        try:
            response = requests.request(
                method,
                f"{self.base_url}{path}",
                timeout=self.timeout,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise ValueError(
                f"No se pudo conectar a georoute ({self.base_url}): {exc}"
            ) from exc

        if not response.ok:
            raise ValueError(
                f"georoute respondió {response.status_code}: {response.text}"
            )

        try:
            return response.json()
        except ValueError as exc:
            raise ValueError("georoute devolvió una respuesta que no es JSON") from exc


class GeorouteTransitClient:
    """Consulta el servidor multimodal Rust basado en RAPTOR y GTFS."""

    def __init__(self):
        self.base_url = os.getenv(
            "GEOROUTE_TRANSIT_URL",
            "http://georoute-transit:8090",
        ).rstrip("/")
        self.timeout = float(os.getenv("GEOROUTE_TIMEOUT_SECONDS", "60"))

    def isochrone(
        self,
        lon: float,
        lat: float,
        minutes: float,
        departure_hour: int | None = None,
    ) -> dict[str, Any]:
        params = {
            "from": f"{lon},{lat}",
            "minutes": str(minutes),
        }
        if departure_hour is not None:
            params["departure_hour"] = str(departure_hour)
        return self._request("get", "/isochrone", params=params)

    def isochrones(
        self,
        origins: list[list[float]],
        minutes: float,
        departure_hour: int | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "origins": origins,
            "minutes": minutes,
        }
        if departure_hour is not None:
            payload["departure_hour"] = departure_hour
        return self._request("post", "/isochrones", json=payload)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        try:
            response = requests.request(
                method,
                f"{self.base_url}{path}",
                timeout=self.timeout,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise ValueError(
                f"No se pudo conectar a georoute transit ({self.base_url}): {exc}"
            ) from exc

        if not response.ok:
            raise ValueError(
                f"georoute transit respondió {response.status_code}: {response.text}"
            )

        try:
            return response.json()
        except ValueError as exc:
            raise ValueError(
                "georoute transit devolvió una respuesta que no es JSON"
            ) from exc
