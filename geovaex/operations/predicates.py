import pygeos as pg
import concurrent.futures
import numpy as np


class PredicateFilters:
    """Predicate Filters.

    Wrapper to numpy array result of predicates, to use in DataFrame filtering.
    """
    def __init__(self, filt, name):
        self._filter = filt
        self._name = name

    @property
    def filter(self):
        return self._filter

    def to_numpy(self):
        return self._filter

    @property
    def name(self):
        return self._name

    def __repr__(self):
        return self._filter.__repr__()

    def __getitem__(self, item):
        return self._filter.__getitem__(item)


def _geom_to_pygeos(geom):
    if isinstance(geom, str):
        geom = pg.from_wkt(geom)
    elif isinstance(geom, bytes):
        geom = pg.from_wkb(geom)
    elif isinstance(geom, pg.lib.Geometry):
        pass
    else:
        raise ValueError("'geom' should be WKT, WKB or pygeos Geometry.")
    return geom


class Predicates:
    """Spatial Predicates.

    Applies PyGEOS predicates to DataFrame geometry. For more, see https://pygeos.readthedocs.io/en/latest/predicates.html
    """

    def __init__(self, df):
        self._df = df

    def _multithread(self, function, chunks, *args, **kwargs):
        results = [False]*len(chunks)
        nthreads = kwargs.pop('nthreads', None)
        executor = concurrent.futures.ThreadPoolExecutor(nthreads)
        futures = [executor.submit(function, thread_index, pg.from_wkb(group), *args, **kwargs) for thread_index, group in enumerate(chunks)]
        for f in concurrent.futures.as_completed(futures):
            thread_index, result = f.result()
            results[thread_index] = result
        return results

    def _predicate(self, func, *args, **kwargs):
        chunksize = 1000
        chunks = self._df.geometry.chunked(chunksize=chunksize)
        nthreads = self._df.executor.thread_pool.nthreads
        def indexed_func(thread_index, *args, **kwargs):
            result = func(*args, **kwargs)
            return (thread_index, result)
        pieces = self._multithread(indexed_func, chunks, *args, nthreads=nthreads, **kwargs)
        return np.concatenate(pieces)

    def contains(self, geom):
        """Returns True for elements where geom is completely inside GeoDataFrame geometry.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.contains, geom)
        return PredicateFilters(filt, 'contains')

    def contains_properly(self, geom):
        """Returns True for elements where geom is completely inside GeoDataFrame geometry, with no common boundary points.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.contains_properly, geom)
        return PredicateFilters(filt, 'contains_properly')

    def covered_by(self, geom):
        """Returns True for elements where no point in GeoDataFrame geometry is outside geom.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.covered_by, geom)
        return PredicateFilters(filt, 'covered_by')

    def covers(self, geom):
        """Returns True for elements where no point in geom is outside GeoDataFrame geometry.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.covers, geom)
        return PredicateFilters(filt, 'covers')

    def crosses(self, geom):
        """Returns True for elements where geom and GeoDataFrame geometry spatially cross.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.crosses, geom)
        return PredicateFilters(filt, 'crosses')

    def disjoint(self, geom):
        """Returns True for elements where geom and GeoDataFrame geometry do not share any point in space.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.disjoint, geom)
        return PredicateFilters(filt, 'disjoint')

    def equals(self, geom):
        """Returns True for elements where geom and GeoDataFrame geometry are spatially equal.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.equals, geom)
        return PredicateFilters(filt, 'equals')

    def equals_exact(self, geom, tolerance=0.0):
        """Returns True for elements where geom and GeoDataFrame geometry are structurally equal.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Keyword Arguments:
            tolerance (number): The coordinates are required to be equal within the value of this parameter (default: {0.0}).

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.equals_exact, geom, tolerance=tolerance)
        return PredicateFilters(filt, 'equals_exact')

    def intersects(self, geom):
        """Returns True for elements where geom and GeoDataFrame geometry share any portion of space.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.intersects, geom)
        return PredicateFilters(filt, 'intersects')

    def is_ccw(self):
        """Returns True for elements where the GeoDataFrame geometry is a linestring or linearring and it is counterclockwise.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        filt = self._predicate(pg.is_ccw)
        return PredicateFilters(filt, 'is_ccw')

    def is_closed(self):
        """Returns True for elements where the GeoDataFrame geometry is a linestring and its first and last points are equal.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        filt = self._predicate(pg.is_closed)
        return PredicateFilters(filt, 'is_closed')

    def is_empty(self):
        """Returns True for elements where the GeoDataFrame geometry is an empty point, polygon, etc.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        filt = self._predicate(pg.is_empty)
        return PredicateFilters(filt, 'is_empty')

    def is_missing(self):
        """Returns True for elements where the GeoDataFrame geometry object is not a geometry (None).

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        filt = self._predicate(pg.is_missing)
        return PredicateFilters(filt, 'is_missing')

    def is_prepared(self):
        """Returns True for elements where the GeoDataFrame geometry is prepared.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        filt = self._predicate(pg.is_prepared)
        return PredicateFilters(filt, 'is_prepared')

    def is_ring(self):
        """Returns True for elements where the GeoDataFrame geometry is closed and simple.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        filt = self._predicate(pg.is_ring)
        return PredicateFilters(filt, 'is_ring')

    def is_simple(self):
        """Returns True for elements where the GeoDataFrame geometry has no anomalous geometric points, such as self-intersections or self tangency.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        filt = self._predicate(pg.is_simple)
        return PredicateFilters(filt, 'is_simple')

    def is_valid(self):
        """Returns True for elements where the GeoDataFrame geometry is well formed.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        filt = self._predicate(pg.is_valid)
        return PredicateFilters(filt, 'is_valid')

    def overlaps(self, geom):
        """Returns True for elements where geom and GeoDataFrame geometry spatially overlap.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.overlaps, geom)
        return PredicateFilters(filt, 'overlaps')

    def relate_pattern(self, geom, pattern):
        """Returns True for elements where the DE-9IM string code for the relationship between geom and GeoDataFrame geometry satisfies the pattern.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.
            pattern (str): The DE-9IM pattern.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.relate_pattern, geom, pattern)
        return PredicateFilters(filt, 'relate_pattern')

    def touches(self, geom):
        """Returns True for elements where the only points shared between geom and GeoDataFrame geometry are on their boundary.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.touches, geom)
        return PredicateFilters(filt, 'touches')

    def within(self, geom):
        """Returns True for elements where GeoDataFrame geometry is completely inside geom.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The geometry for the predicate.

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        geom = _geom_to_pygeos(geom)
        filt = self._predicate(pg.within, geom)
        return PredicateFilters(filt, 'within')

    def has_type(self, type_):
        """Returns True for geometries of the specific type(s).

        Arguments:
            type (int|array): Geometric type.
                - POINT: 0
                - LINESTRING: 1
                - LINEARRING: 2
                - POLYGON: 3
                - MULTIPOINT: 4
                - MULTILINESTRING: 5
                - MULTIPOLYGON: 6
                - GEOMETRYCOLLECTION: 7

        Returns:
            (PredicateFilters): A boolean array with the predicate for each index.
        """
        if isinstance(type_, int):
            type_ = [type_]
        assert isinstance(type_, list)
        filt = self._predicate(pg.get_type_id)
        def filtToBool(elem):
            return elem in type_
        filtToBoolVec = np.vectorize(filtToBool)
        filt = filtToBoolVec(filt)
        return PredicateFilters(filt, 'geometric_type')
