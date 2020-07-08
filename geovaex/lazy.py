from pyarrow import ChunkedArray, Array
import pygeos as pg

class Lazy(object):
    def __init__(self, function):
        self._function = function
        self._obj = None

    def __call__(self, obj):
        self._obj = obj
        return self

    def __repr__(self):
        return self._head_and_tail_table()

    def __getitem__(self, item):
        if isinstance(item, int):
            item = slice(item, item + 1)
            return self._function(self._obj.__getitem__(item))[0]
        return self._function(self._obj.__getitem__(item))

    def __len__(self):
        return len(self._obj)

    def values(self):
        return self._function(self._obj)

    def _head_and_tail_table(self, n=5, format='plain', to_wkt=True):
        N = len(self._obj)
        if N <= n * 2:
            table = self._as_table(0, N, format=format)
        else:
            table = self._as_table(0, n, N - n, N, format=format)
        expression = "Expression = %s" % (self._function.__name__)
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
