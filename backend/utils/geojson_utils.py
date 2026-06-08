from shapely.geometry import MultiPoint, Point, mapping


def build_isochrone_polygon_from_graph(graph, buffer_degrees: float = 0.002):
    """
    Construye una geometría de isócrona aproximada a partir de los nodos
    del subgrafo alcanzable.

    La geometría final se genera con convex hull sobre los nodos alcanzables.
    Si quedan muy pocos puntos, se usa buffer para evitar geometrías inválidas.
    """
    coords = []

    for _, data in graph.nodes(data=True):
        x = data.get("x")
        y = data.get("y")
        if x is not None and y is not None:
            coords.append((x, y))

    if not coords:
        return None

    if len(coords) == 1:
        return Point(coords[0]).buffer(buffer_degrees)

    geometry = MultiPoint(coords).convex_hull

    # Si el hull queda como punto o línea, se le aplica buffer para formar polígono
    if geometry.geom_type == "Point":
        geometry = geometry.buffer(buffer_degrees)
    elif geometry.geom_type == "LineString":
        geometry = geometry.buffer(buffer_degrees)

    return geometry


def geometry_to_feature(geometry, properties=None):
    """
    Convierte una geometría de Shapely a una feature GeoJSON.
    """
    return {
        "type": "Feature",
        "geometry": mapping(geometry),
        "properties": properties or {}
    }


def point_to_feature(lon: float, lat: float, properties=None):
    """
    Convierte un punto lon/lat a feature GeoJSON.
    """
    return geometry_to_feature(Point(lon, lat), properties=properties)


def feature_collection(features, center_list=None,metadata=None):
    """
    Arma un FeatureCollection GeoJSON.
    Se deja metadata como campo adicional para no perder información útil.
    Si se proporciona center_list, convierte los centros a features Point
    """
    if center_list:
        for center in center_list:
            lon = center.get("lng")
            lat = center.get("lat")

            if lon is not None and lat is not None:
                center_feature = point_to_feature(
                    lon,
                    lat,
                    properties={
                        "kind": "health_center",
                        "name": center.get("nombre", "Centro de salud")
                    }
                )
                features.append(center_feature)

    payload = {
        "type": "FeatureCollection",
        "features": features
    }
    if metadata is not None:
        payload["metadata"] = metadata
    return payload