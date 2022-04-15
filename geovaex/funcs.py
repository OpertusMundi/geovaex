import pygeos as pg
from .lazy import Lazy, LazyObj
import numpy as np
import pyproj
import warnings


@Lazy
def from_wkb(arr):
    return pg.from_wkb(arr)


@Lazy
def to_wkt(arr, **kwargs):
    return pg.to_wkt(from_wkb(arr), **kwargs)


@Lazy
def to_wkb(arr):
    return pg.to_wkb(arr)


@Lazy
def convex_hull(arr):
    return pg.to_wkb(pg.convex_hull(pg.from_wkb(arr)))


@Lazy
def get_coordinates(arr):
    return pg.get_coordinates(pg.from_wkb(arr))


@Lazy
def get_inverted_coordinates(arr):
    def invert_list(a):
        if isinstance(a[0], np.ndarray):
            return np.array([np.flip(array) for array in a])
        else:
            return np.flip(a)
    return invert_list(pg.get_coordinates(pg.from_wkb(arr)))


@Lazy
def transform(arr, src_crs, tgt_crs):
    transformer = pyproj.Transformer.from_crs(src_crs, tgt_crs, always_xy=True)

    geometry = pg.from_wkb(arr)
    coords = pg.get_coordinates(geometry)
    new_coords = transformer.transform(coords[:, 0], coords[:, 1])
    projected = pg.set_coordinates(geometry, np.array(new_coords).T)
    return pg.to_wkb(projected)


def total_bounds(arr):
    if isinstance(arr, LazyObj):
        arr = arr.values()
    return pg.to_wkb(pg.box(*pg.total_bounds(pg.from_wkb(arr))))


def union_all(arr):
    if isinstance(arr, LazyObj):
        arr = arr.values()
    return pg.union_all(from_wkb(arr))


def convex_hull_all(arr):
    if isinstance(arr, LazyObj):
        arr = arr.values()
    points = pg.union_all(pg.extract_unique_points(pg.from_wkb(arr)))
    return pg.to_wkb(pg.convex_hull(points))


@Lazy
def extract_unique_points(arr):
    return pg.extract_unique_points(from_wkb(arr))


def within(arr, geometry):
    geometry = pg.from_wkb(geometry)
    return pg.within(from_wkb(arr), geometry)


@Lazy
def constructive(arr, operation, *args, **kwargs):
    if operation == 'boundary':
        geometries = pg.boundary(pg.from_wkb(arr), **kwargs)
    elif operation == 'buffer':
        geometries = pg.buffer(pg.from_wkb(arr), *args, **kwargs)
    elif operation == 'build_area':
        geometries = pg.build_area(pg.from_wkb(arr), **kwargs)
    elif operation == 'centroid':
        geometries = pg.centroid(pg.from_wkb(arr), **kwargs)
    elif operation == 'clip_by_rect':
        geometries = pg.clip_by_rect(pg.from_wkb(arr), *args, **kwargs)
    elif operation == 'convex_hull':
        geometries = pg.convex_hull(pg.from_wkb(arr), **kwargs)
    elif operation == 'delaunay_triangles':
        geometries = pg.delaunay_triangles(pg.from_wkb(arr), **kwargs)
    elif operation == 'envelope':
        geometries = pg.envelope(pg.from_wkb(arr), **kwargs)
    elif operation == 'extract_unique_points':
        geometries = pg.extract_unique_points(pg.from_wkb(arr), **kwargs)
    elif operation == 'make_valid':
        geometries = pg.make_valid(pg.from_wkb(arr), **kwargs)
    elif operation == 'normalize':
        geometries = pg.normalize(pg.from_wkb(arr), **kwargs)
    elif operation == 'offset_curve':
        geometries = pg.offset_curve(pg.from_wkb(arr), *args, **kwargs)
    elif operation == 'point_on_surface':
        geometries = pg.point_on_surface(pg.from_wkb(arr), **kwargs)
    elif operation == 'reverse':
        geometries = pg.reverse(pg.from_wkb(arr), **kwargs)
    elif operation == 'simplify':
        geometries = pg.simplify(pg.from_wkb(arr), *args, **kwargs)
    elif operation == 'snap':
        geometries = pg.snap(pg.from_wkb(arr), *args, **kwargs)
    elif operation == 'voronoi_polygons':
        geometries = pg.voronoi_polygons(pg.from_wkb(arr), **kwargs)
    else:
        warnings.warn(f'Operation {operation} not supported.')
        return None
    return pg.to_wkb(geometries)
