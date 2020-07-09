import pygeos as pg
import pyarrow as pa
from .lazy import Lazy, LazyObj
import numpy as np
import pyproj

@Lazy
def from_wkb(arr):
    return pg.from_wkb(arr)

@Lazy
def to_wkt(arr):
    return pg.to_wkt(from_wkb(arr))

@Lazy
def to_wkb(arr):
    return pg.to_wkb(arr)

@Lazy
def convex_hull(arr):
    return pg.to_wkb(pg.convex_hull(pg.from_wkb(arr)))

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
