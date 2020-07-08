import pyarrow as pa
import pygeos as pg
import concurrent.futures
from .funcs import *

class GeoSeries(object):

    def __init__(self, geometry):
        self._geometry = geometry
        self._active_fraction = 1
        self._index_start = 0
        self._length_original = len(geometry)
        self._length = self._length_original
        self._index_end = self._length

    @property
    def _active_geometry(self):
        if self._length != self._length_original:
            return self._geometry[self._index_start:self._index_end]
        return self._geometry

    def __repr__(self):
        return self._head_and_tail_table(format='plain')

    def __getitem__(self, item):
        if isinstance(item, int):
            piece = self._active_geometry.__getitem__(slice(item, item+1))
            if isinstance(piece, pa.ChunkedArray):
                piece = piece.chunk(0)
            return piece
        elif isinstance(item, slice):
            start, stop, step = item.start, item.stop, item.step
            start = start or 0
            stop = stop or len(self)
            if start < 0:
                start = len(self)+start
            if stop < 0:
                stop = len(self)+stop
            stop = min(stop, len(self))
            assert step in [None, 1]
            gs = self.trim()
            gs.set_active_range(start, stop)
            return gs.trim()

    def __len__(self):
        return self._length

    def get_raw_geometry(self):
        return self._active_geometry

    def copy(self):
        gs = GeoSeries(self._geometry)
        gs._active_fraction = self._active_fraction
        gs._index_start = self._index_start
        gs._length_original = self._length_original
        gs._length = self._length
        gs._index_end = self._index_end
        return gs

    def trim(self, inplace=False):
        gs = self if inplace else self.copy()
        if self._index_start == 0 and len(self._geometry) == self._index_end:
            pass  # we already assigned it in .copy
        else:
            gs._geometry = self._geometry[self._index_start:self._index_end]
        gs._active_fraction = 1
        gs._index_start = 0
        gs._length_original = self._index_end - self._index_start
        gs._length = gs._length_original
        gs._index_end = gs._length
        return gs

    def length_original(self):
        return self._length_original

    def get_active_range(self):
        return self._index_start, self._index_end

    def set_active_range(self, i1, i2):
        self._active_fraction = (i2 - i1) / float(self.length_original())
        self._index_start = i1
        self._index_end = i2
        self._length = i2 - i1

    def chunked(self, chunksize=1000000):
        offset = self._index_start
        lower = offset
        chunks = []
        for i in range(1, self._length//chunksize + 2):
            upper = min(i*chunksize + offset, self._length + offset)
            if upper <= lower:
                continue
            chunks.append(self._geometry[lower:upper])
            lower = upper
        return chunks

    def _repr_html_(self):
        return self._head_and_tail_table()

    def _head_and_tail_table(self, n=5, format='html'):
        N = len(self)
        if N <= n * 2:
            return self._as_table(0, N, format=format)
        else:
            return self._as_table(0, n, N - n, N, format=format)

    def _as_table(self, i1, i2, j1=None, j2=None, format='html'):
        import tabulate

        values_list = []
        if i2 - i1 > 0:
            for i in range(i1, i2):
                idx = "<i style='opacity: 0.6'>{:,}</i>".format(i)
                values_list.append([idx, to_wkt(self[i])[0]])
            if j1 is not None and j2 is not None:
                values_list.append(['...', '...'])
                for i in range(j1, j2):
                    idx = "<i style='opacity: 0.6'>{:,}</i>".format(i)
                    values_list.append([idx, to_wkt(self[i])[0]])

        table_text = str(tabulate.tabulate(values_list, headers=["#", "geometry"], tablefmt=format))
        if tabulate.__version__ == '0.8.7':
            # Tabulate 0.8.7 escapes html :()
            table_text = table_text.replace('&lt;i style=&#x27;opacity: 0.6&#x27;&gt;', "<i style='opacity: 0.6'>")
            table_text = table_text.replace('&lt;/i&gt;', "</i>")
        return table_text

    def take(self, indices):
        offset = 0
        chunks = []
        for chunk in self._active_geometry.chunks:
            size = len(chunk)
            chunk_indices = list(filter(lambda x: offset <= x < size + offset, indices))
            chunk_indices = pa.array(map(lambda x: x - offset, chunk_indices))
            if len(chunk_indices) > 0:
                chunks.append(chunk.take(chunk_indices))
            offset += size
        if len(chunks) > 0:
            geometry = pa.concat_arrays(chunks)
            return GeoSeries(geometry=geometry)
        else:
            raise IndexError('ERROR: Out of range')

    def to_pygeos(self):
        return from_wkb(self._active_geometry)

    def union_all(self):
        return union_all(self._active_geometry)

    def convex_hull(self):
        return GeoSeries(geometry=convex_hull(self._active_geometry))

    def vertices(self):
        return extract_unique_points(self._active_geometry)

    def all_vertices(self):
        return self.vertices().union_all()

    def _multiprocess(self, function, chunks, max_workers=None):
        result = []
        executor = concurrent.futures.ProcessPoolExecutor(max_workers)
        futures = [executor.submit(function, group) for group in chunks]
        for f in concurrent.futures.as_completed(futures):
            result.append(f.result())
        return GeoSeries(pa.array(result))

    def _total_bounds_single(self):
        return pg.box(*pg.total_bounds(self.to_pygeos()))

    def total_bounds(self, chunksize=1000000, max_workers=None):
        chunks = self.chunked(chunksize)
        if max_workers == 1 or len(chunks) == 1:
            return self._total_bounds_single()
        bounds = self._multiprocess(total_bounds, chunks, max_workers=max_workers)
        return bounds.total_bounds(chunksize=chunksize, max_workers=max_workers)

    def _convex_hull_all_single(self):
        return pg.from_wkb(convex_hull_all(self._active_geometry))

    def convex_hull_all(self, chunksize=50000, max_workers=None):
        chunks = self.chunked(chunksize)
        if max_workers == 1 or len(chunks) == 1:
            return self._convex_hull_all_single()
        hulls = self._multiprocess(convex_hull_all, chunks, max_workers=max_workers)
        return hulls.convex_hull_all(chunksize=chunksize, max_workers=max_workers)
