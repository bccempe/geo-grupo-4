from shapely.geometry import MultiPoint, Point, mapping


def build_isochrone_polygon_from_graph(
    graph,
    boundary=None,
    buffer_degrees: float = 0.002
):
    """
    Construye un polígono de isócrona a partir de los nodos alcanzables.

    Luego recorta el resultado utilizando el límite comunal para evitar
    que la isócrona sobresalga fuera de la comuna.
    """

    coords = []

    for _, data in graph.nodes(data=True):

        x = data.get("x")
        y = data.get("y")

        if x is not None and y is not None:
            coords.append((x, y))

    if not coords:
        return None

    # ===============================
    # Muy pocos puntos
    # ===============================

    if len(coords) == 1:
        geometry = Point(coords[0]).buffer(buffer_degrees)

    else:

        geometry = MultiPoint(coords).convex_hull

        if geometry.geom_type == "Point":
            geometry = geometry.buffer(buffer_degrees)

        elif geometry.geom_type == "LineString":
            geometry = geometry.buffer(buffer_degrees)

    # ===============================
    # Recorte comunal
    # ===============================

    if boundary is not None:

        geometry = geometry.intersection(boundary)

        if not geometry.is_valid:
            geometry = geometry.buffer(0)

    return geometry


def geometry_to_feature(geometry, properties=None):

    return {
        "type": "Feature",
        "geometry": mapping(geometry),
        "properties": properties or {}
    }


def point_to_feature(lon, lat, properties=None):

    return geometry_to_feature(
        Point(lon, lat),
        properties
    )


def feature_collection(features, center_list=None, metadata=None):

    if center_list:

        for center in center_list:

            lon = center.get("lng", center.get("lon"))
            lat = center.get("lat")

            if lon is None or lat is None:
                continue

            features.append(
                point_to_feature(
                    lon,
                    lat,
                    properties={
                        "kind": "health_center",
                        "name": center.get(
                            "nombre",
                            center.get("name", "Centro de salud")
                        )
                    }
                )
            )

    payload = {
        "type": "FeatureCollection",
        "features": features
    }

    if metadata is not None:
        payload["metadata"] = metadata

    return payload
