import pyarrow as pa
import pygeos as pg
import numpy as np
import concurrent.futures
import pyproj
from .funcs import *
from .lazy import LazyObj

class GeoSeries(object):

    def __init__(self, geometry, crs=None, df=None):
        self._geometry = geometry
        self._crs = crs if crs is None or isinstance(crs, pyproj.crs.crs.CRS) else pyproj.crs.CRS.from_user_input(crs)
        self._active_fraction = df._active_fraction if df is not None else 1
        self._index_start = df._index_start if df is not None else 0
        self._length_original = df._length_original or len(geometry) if df is not None else len(geometry)
        self._length_unfiltered = df._length_unfiltered or self._length_original if df is not None else self._length_original
        self._index_end = df._index_end or self._length_original if df is not None else self._length_original
        self._df = df

    @property
    def _active_geometry(self):
        geometry = self._geometry
        if self.filtered:
            mask = pa.array(self._df.evaluate_selection_mask(None))
            geometry = geometry.filter(mask)

        return geometry

    @property
    def filtered(self):
        return self._df.filtered if self._df is not None else False

    @property
    def crs(self):
        return self._crs

    @crs.setter
    def crs(self, crs):
        self._crs = crs if crs is None or isinstance(crs, pyproj.crs.crs.CRS) else pyproj.crs.CRS.from_user_input(crs)

    def to_crs(self, crs):
        if self.crs is None:
            self.crs = crs
        else:
            self._geometry = transform(self._geometry, self.crs, crs)
            self.crs = crs

    def __repr__(self):
        return self._head_and_tail_table(format='plain')

    def __getitem__(self, item):
        if isinstance(item, int):
            piece = self._active_geometry.__getitem__(slice(item, item+1))
            if isinstance(piece, pa.ChunkedArray):
                piece = piece.chunk(0)
            return pg.from_wkb(piece)[0]
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
            if self.filtered:
                count_check = self._df.count()  # fill caches and masks
                mask = self._df._selection_masks['__filter__']
                start, stop = mask.indices(start, stop-1) # -1 since it is inclusive
                assert start != -1
                assert stop != -1
                stop = stop+1  # +1 to make it inclusive
            gs = self.trim()
            gs.set_active_range(start, stop)
            return gs.trim()

    def __len__(self):
        return self._length_unfiltered if not self.filtered else len(self._df)

    def get_raw_geometry(self):
        return self._active_geometry

    def copy(self, df=None):
        geometry = self._geometry.copy() if isinstance(self._geometry, LazyObj) else self._geometry
        gs = GeoSeries(geometry, crs=self._crs)
        gs._active_fraction = self._active_fraction
        gs._index_start = self._index_start
        gs._length_original = self._length_original
        gs._length_unfiltered = self._length_unfiltered
        gs._index_end = self._index_end
        gs._df = self._df
        return gs

    def trim(self, inplace=False):
        gs = self if inplace else self.copy()
        if self._index_start == 0 and len(self._geometry) == self._index_end:
            pass  # we already assigned it in .copy
        else:
            gs._geometry = gs._geometry[self._index_start:self._index_end]
        gs._active_fraction = 1
        gs._index_start = 0
        gs._length_original = self._length_unfiltered
        gs._length_unfiltered = gs._length_original
        gs._index_end = gs._length_original
        return gs

    def length_original(self):
        return self._length_original

    def get_active_range(self):
        return self._index_start, self._index_end

    def set_active_range(self, i1, i2):
        self._active_fraction = (i2 - i1) / float(self.length_original())
        self._index_start = i1
        self._index_end = i2
        self._length_unfiltered = i2 - i1

    def chunked(self, chunksize=1000000):
        offset = self._index_start
        lower = offset
        chunks = []
        for i in range(1, self._length_unfiltered//chunksize + 2):
            upper = min(i*chunksize + offset, self._length_unfiltered + offset)
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
                values_list.append([idx, self[i]])
            if j1 is not None and j2 is not None:
                values_list.append(['...', '...'])
                for i in range(j1, j2):
                    idx = "<i style='opacity: 0.6'>{:,}</i>".format(i)
                    values_list.append([idx, self[i]])

        table_text = str(tabulate.tabulate(values_list, headers=["#", "geometry"], tablefmt=format))
        if tabulate.__version__ == '0.8.7':
            # Tabulate 0.8.7 escapes html :()
            table_text = table_text.replace('&lt;i style=&#x27;opacity: 0.6&#x27;&gt;', "<i style='opacity: 0.6'>")
            table_text = table_text.replace('&lt;/i&gt;', "</i>")
        return table_text

    def take(self, indices, filtered=True):
        gs = self.trim()
        if gs.filtered and filtered:
            # we translate the indices that refer to filters row indices to
            # indices of the unfiltered row indices
            indices = np.asarray(indices)
            gs._df.count() # make sure the mask is filled
            max_index = indices.max()
            mask = gs._df._selection_masks['__filter__']
            filtered_indices = mask.first(max_index+1)
            indices = filtered_indices[indices]
        if isinstance(gs._geometry, pa.ChunkedArray):
            offset = 0
            chunks = []
            for chunk in gs._geometry.chunks:
                size = len(chunk)
                chunk_indices = list(filter(lambda x: offset <= x < size + offset, indices))
                chunk_indices = pa.array(map(lambda x: x - offset, chunk_indices))
                if len(chunk_indices) > 0:
                    chunks.append(chunk.take(chunk_indices))
                offset += size
            if len(chunks) > 0:
                geometry = pa.concat_arrays(chunks)
            else:
                raise IndexError('ERROR: Out of range')
        elif isinstance(gs._geometry, pa.Array):
            indices = pa.array(indices)
            geometry = gs._geometry.take(indices)
        else:
            geometry = gs._geometry.take(indices)
        return GeoSeries(geometry=geometry, crs=gs._crs)

    def to_pygeos(self):
        return from_wkb(self._active_geometry)

    def union_all(self):
        return union_all(self._active_geometry)

    def convex_hull(self):
        gs = self.trim()
        gs._geometry = convex_hull(gs._geometry)
        return gs

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
        return result

    def _total_bounds_single(self):
        return pg.box(*pg.total_bounds(self.to_pygeos()))

    def total_bounds(self, chunksize=1000000, max_workers=None):
        chunks = self.chunked(chunksize)
        if len(chunks) == 1:
            return self._total_bounds_single()
        bounds = self._multiprocess(total_bounds, chunks, max_workers=max_workers)
        bounds = GeoSeries(pa.array(bounds), crs=self._crs, df=self._df)
        return bounds.total_bounds(chunksize=chunksize, max_workers=max_workers)

    def _convex_hull_all_single(self):
        return pg.from_wkb(convex_hull_all(self._active_geometry))

    def convex_hull_all(self, chunksize=50000, max_workers=None):
        chunks = self.chunked(chunksize)
        if len(chunks) == 1:
            return self._convex_hull_all_single()
        hulls = self._multiprocess(convex_hull_all, chunks, max_workers=max_workers)
        hulls = GeoSeries(pa.array(hulls), crs=self._crs, df=self._df)
        return hulls.convex_hull_all(chunksize=chunksize, max_workers=max_workers)
