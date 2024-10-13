from typing import List

from shapely import (  # type: ignore
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
    LinearRing,
)
from shapely.wkb import dumps as wkb_dumps  # type: ignore

from dlt.common.typing import DictStrStr


def generate_sample_geometry_records() -> List[DictStrStr]:
    """
    Generate sample geometry records including WKT and WKB representations.

    Returns:
        A list of dictionaries, each containing a geometry type,
        its Well-Known Text (WKT), and Well-Known Binary (WKB) representation.
    """
    geometries = [
        ("Point", Point(1, 1)),
        ("LineString", LineString([(0, 0), (1, 1), (2, 2)])),
        ("Polygon", Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])),
        ("MultiPoint", MultiPoint([(0, 0), (1, 1), (2, 2)])),
        ("MultiLineString", MultiLineString([((0, 0), (1, 1)), ((2, 2), (3, 3))])),
        (
            "MultiPolygon",
            MultiPolygon(
                [
                    Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
                    Polygon([(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]),
                ]
            ),
        ),
        (
            "GeometryCollection",
            GeometryCollection([Point(1, 1), LineString([(0, 0), (1, 1), (2, 2)])]),
        ),
        ("LinearRing", LinearRing([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])),
        (
            "ComplexPolygon",
            Polygon(
                [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
                [[(4, 4), (6, 4), (6, 6), (4, 6), (4, 4)]],
            ),
        ),
        ("EmptyPoint", Point()),
        ("EmptyLineString", LineString()),
        ("EmptyPolygon", Polygon()),
        ("EmptyMultiPoint", MultiPoint()),
        ("EmptyMultiLineString", MultiLineString()),
        ("EmptyMultiPolygon", MultiPolygon()),
        ("EmptyGeometryCollection", GeometryCollection()),
    ]
    return [
        {"type": f"{name}_{format_}", "geom": getattr(geom, format_)}
        for name, geom in geometries
        for format_ in ("wkt", "wkb", "wkb_hex")
    ]
