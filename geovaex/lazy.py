from pyarrow import ChunkedArray, Array
import pygeos as pg

class LazyObj(object):

    def __init__(self):
        self._function = []
        self._obj = None
        self._args = []
        self._counter = 0

    @classmethod
    def init(cls, function, obj, *args):
        lz = cls()
        lz._function.append(function)
        lz._obj = obj
        lz._args.append(args)
        lz._counter += 1
        return lz

    def copy(self):
        lz = LazyObj()
        lz._function = [*self._function]
        lz._obj = self._obj
        lz._args = [*self._args]
        lz._counter = self._counter
        return lz

    def add(self, function, *args):
        lz = self.copy()
        lz._function.append(function)
        lz._args.append(args)
        lz._counter += 1
        return lz

    def __repr__(self):
        return self._head_and_tail_table()

    def __getitem__(self, item):
        if isinstance(item, int):
            item = slice(item, item + 1)
            result = self._obj.__getitem__(item)
            for i in range(self._counter):
                result = self._function[i](result, *self._args[i])
            return result[0]
        result = self._obj.__getitem__(item)
        for i in range(self._counter):
            result = self._function[i](result, *self._args[i])
        return result

    def __len__(self):
        return len(self._obj)

    def values(self):
        result = self._obj
        for i in range(self._counter):
            result = self._function[i](result)
        return result

    def _head_and_tail_table(self, n=5, format='plain', to_wkt=True):
        N = len(self._obj)
        if N <= n * 2:
            table = self._as_table(0, N, format=format)
        else:
            table = self._as_table(0, n, N - n, N, format=format)
        expression = "Expression = %s" % '*'.join(self._function[i-1].__name__ for i in range(self._counter, 0, -1))
        head = "Length: {:,} type: {}".format(N, type(self[0]))
        line = ''
        for i in range(len(head)):
            line += '-'
        return expression + "\n" + head + "\n" + line + "\n" + table

    def _as_table(self, i1, i2, j1=None, j2=None, format='plain', to_wkt=True):
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

class Lazy(object):
    def __init__(self, function):
        self._function = function

    def __call__(self, obj, *args):
        if isinstance(obj, LazyObj):
            return obj.add(self._function, *args)
        return LazyObj.init(self._function, obj, *args)
