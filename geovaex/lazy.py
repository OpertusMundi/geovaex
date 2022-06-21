from pyarrow import ChunkedArray, Array, array, concat_arrays
import pygeos as pg


class LazyObj:

    def __init__(self):
        self._function = []
        self._obj = None
        self._args = []
        self._kwargs = []
        self._counter = 0

    @classmethod
    def init(cls, function, obj, *args, **kwargs):
        lz = cls()
        assert isinstance(obj, (ChunkedArray, Array))
        lz._function.append(function)
        lz._obj = obj
        lz._args.append(args)
        lz._kwargs.append(kwargs)
        lz._counter += 1
        return lz

    def copy(self):
        lz = LazyObj()
        lz._function = [*self._function]
        lz._obj = self._obj
        lz._args = [*self._args]
        lz._kwargs = [*self._kwargs]
        lz._counter = self._counter
        return lz

    def add(self, function, *args, **kwargs):
        lz = self.copy()
        lz._function.append(function)
        lz._args.append(args)
        lz._kwargs.append(kwargs)
        lz._counter += 1
        return lz

    def take(self, indices):
        lz = self.copy()
        if isinstance(lz._obj, ChunkedArray):
            offset = 0
            chunks = []
            for chunk in lz._obj.chunks:
                size = len(chunk)
                chunk_indices = [x for x in indices if offset <= x < size + offset]
                chunk_indices = array([x - offset for x in chunk_indices])
                if len(chunk_indices) > 0:
                    chunks.append(chunk.take(chunk_indices))
                offset += size
            if len(chunks) > 0:
                obj = concat_arrays(chunks)
            else:
                raise IndexError('ERROR: Out of range')
        else:
            indices = array(indices)
            obj = lz._obj.take(indices)
        lz._obj = obj
        return lz

    def filter(self, arr):
        assert len(self) == len(arr)
        lz = self.copy()
        lz._obj = lz._obj.filter(arr)
        return lz

    def __repr__(self):
        return self._head_and_tail_table()

    def __getitem__(self, item):
        if isinstance(item, int):
            item = slice(item, item + 1)
            result = self._obj.__getitem__(item)
            for i in range(self._counter):
                result = self._function[i](result, *self._args[i], **self._kwargs[i])
            return result[0]
        result = self._obj.__getitem__(item)
        for i in range(self._counter):
            result = self._function[i](result, *self._args[i], **self._kwargs[i])
        return result

    def __len__(self):
        return len(self._obj)

    def values(self):
        result = self._obj
        for i in range(self._counter):
            result = self._function[i](result, *self._args[i], **self._kwargs[i])
        return result

    def to_numpy(self):
        return self.values()

    def _head_and_tail_table(self, n=5, format='plain'):
        N = len(self._obj)
        if N <= n * 2:
            table = self._as_table(0, N, format=format)
        else:
            table = self._as_table(0, n, N - n, N, format=format)
        expression = "Expression = %s" % '*'.join(self._function[i-1].__name__ for i in range(self._counter, 0, -1))
        head = "Length: {:,} type: {}".format(N, type(self[0]))
        line = ''
        for _ in range(len(head)):
            line += '-'
        return expression + "\n" + head + "\n" + line + "\n" + table

    def _as_table(self, i1, i2, j1=None, j2=None, format='plain'):
        import tabulate

        values_list = []
        if i2 - i1 > 0:
            for i in range(i1, i2):
                value = self[i]
                if isinstance(value, (bytes, bytearray)):
                    value = pg.to_wkt(pg.from_wkb(value))
                values_list.append([i, value])
            if j1 is not None and j2 is not None:
                values_list.append(['...'])
                for i in range(j1, j2):
                    value = self[i]
                    if isinstance(value, (bytes, bytearray)):
                        value = pg.to_wkt(pg.from_wkb(value))
                    values_list.append([i, value])

        return str(tabulate.tabulate(values_list, tablefmt=format))


class Lazy:
    def __init__(self, function):
        self._function = function

    def __call__(self, obj, *args, **kwargs):
        if isinstance(obj, LazyObj):
            return obj.add(self._function, *args, **kwargs)
        return LazyObj.init(self._function, obj, *args, **kwargs)
