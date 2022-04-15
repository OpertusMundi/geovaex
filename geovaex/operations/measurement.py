import pygeos as pg
import concurrent.futures
import numpy as np


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


class Measurement:
    """Spatial Measurement.

    Applies PyGEOS predicates to DataFrame geometry. For more, see https://pygeos.readthedocs.io/en/latest/measurement.html
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

    def _measurement(self, func, *args, **kwargs):
        chunksize = 1000
        chunks = self._df.geometry.chunked(chunksize=chunksize)
        nthreads = self._df.executor.thread_pool.nthreads
        def indexed_func(thread_index, *args, **kwargs):
            result = func(*args, **kwargs)
            return (thread_index, result)
        pieces = self._multithread(indexed_func, chunks, *args, nthreads=nthreads, **kwargs)
        return np.concatenate(pieces)

    def area(self):
        """Computes the area of a (multi)polygon.

        Returns:
            (numpy.ndarray): Each element represents the area of the corresponding geometry in the DataFrame.
        """
        return self._measurement(pg.area)

    def bounds(self):
        """Computes the bounds (extent) of a geometry.

        Returns:
            (numpy.ndarray): Array with elements in the form [xmin, ymin, xmax, ymax].
        """
        return self._measurement(pg.bounds)

    def distance(self, geom):
        """Computes the Cartesian distance between two geometries.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The distance between each geometry in the DataFrame and this geometry will be computed. (The coordinate system of the geometry is assumed to be the same as the DataFrame's CRS.)

        Returns:
            (numpy.ndarray): Array with float numbers representing the distance element-wise.
        """
        geom = _geom_to_pygeos(geom)
        return self._measurement(pg.distance, geom)

    def frechet_distance(self, geom, densify=None):
        """Compute the discrete Fréchet distance between two geometries.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The Frechet distance between each geometry in the DataFrame and this geometry will be computed. (The coordinate system of the geometry is assumed to be the same as the DataFrame's CRS.)

        Keyword Arguments:
            densify (float): Represents the coarseness of the discrete approximation (default: {None})

        Returns:
            (numpy.ndarray): Array with float numbers representing the discrete Frechet distance element-wise.
        """
        geom = _geom_to_pygeos(geom)
        return self._measurement(pg.frechet_distance, geom, densify)

    def hausdorff_distance(self, geom, densify=None):
        """Compute the discrete Hausdorff distance between two geometries.

        Arguments:
            geom (str|bytes|pygeos.lib.Geometry): The Hausdorff distance between each geometry in the DataFrame and this geometry will be computed. (The coordinate system of the geometry is assumed to be the same as the DataFrame's CRS.)

        Keyword Arguments:
            densify (float): Represents the coarseness of the discrete approximation (default: {None})

        Returns:
            (numpy.ndarray): Array with float numbers representing the discrete Hausdorff distance element-wise.
        """
        geom = _geom_to_pygeos(geom)
        return self._measurement(pg.hausdorff_distance, geom, densify)

    def length(self):
        """Computes the length of a (multi)linestring or polygon perimeter.

        Returns:
            (numpy.ndarray): Array with float numbers representing the length of the geometry (the length of non linestring or polygon geometries is considered as zero).
        """
        return self._measurement(pg.length)

    def minimum_clearance(self):
        """Computes the Minimum Clearance distance.

        A geometry’s “minimum clearance” is the smallest distance by which a vertex of the geometry could be moved to produce an invalid geometry.

        If no minimum clearance exists for a geometry (for example, a single point, or an empty geometry), infinity is returned.

        Returns:
            (numpy.ndarray): Array with the Minimum Clearance distance for the geometry of each element of the DataFrame.
        """
        return self._measurement(pg.minimum_clearance)

    def total_bounds(self):
        """Computes the total bounds (extent) of the geometry.

        Returns:
            (numpy.ndarray): Array with the extent in the form [xmin, ymin, xmax, ymax] for the geometry of each element of the DataFrame.
        """
        return self._measurement(pg.total_bounds)
