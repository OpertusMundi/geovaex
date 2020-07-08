from .geoseries import GeoSeries
from vaex.dataframe import DataFrameLocal
import geovaex.io

class GeoDataFrame(DataFrameLocal):
    def __init__(self, geometry, crs=None, path=None):
        super(GeoDataFrame, self).__init__(name=path, path=path, column_names=[])
        self._geoseries = GeoSeries(geometry, crs=crs)

    @property
    def geometry(self):
        return self._geoseries

    def __str__(self):
        return self._head_and_tail_table(format='plain')

    def __repr__(self):
        return self._head_and_tail_table(format='plain')

    def _repr_mimebundle_(self, include=None, exclude=None, **kwargs):
        return {'text/html':self._repr_html_(), 'text/plain': self._head_and_tail_table(format='plain')}

    def _repr_html_(self):
        css = """.vaex-description pre {
          max-width : 450px;
          white-space : nowrap;
          overflow : hidden;
          text-overflow: ellipsis;
        }
        .table {
          display: table;
          border-bottom: 3px solid black;
        }
        .row {
          display: table-row;
        }
        .cell {
          display: table-cell;
        }
        .centered {
          text-align: center;
          font-weight: bold;
        }
        .vaex-description pre:hover {
          max-width : initial;
          white-space: pre;
        }"""
        from IPython import display
        style = "<style>%s</style>" % css
        display.display(display.HTML(style))
        table1 = self._head_and_tail_table()
        table2 = self.geometry._head_and_tail_table()
        return '<div class="table"><div class="row"><div class="cell centered">Attributes</div></div><div class="row"><div class="cell">%s</div></div></div><div class="table"><div class="row"><div class="cell centered">Geometry</div></div><div class="row"><div class="cell">%s</div></div></div>' % (table1, table2)

    def __getitem__(self, item):
        if isinstance(item, int):
            names = self.get_column_names()
            row = [self.evaluate(name, item, item+1)[0] for name in names]
            row.append(self.geometry[item])
            return row
        return super(GeoDataFrame, self).__getitem__(item)

    def set_active_range(self, i1, i2):
        super(GeoDataFrame, self).set_active_range(i1, i2)
        self.geometry.set_active_range(i1, i2)

    def take(self, indices):
        geometry = self.geometry.take(indices)
        return geovaex.from_df(geometry=geometry.get_raw_geometry(), crs=self.geometry.crs, df=super(GeoDataFrame, self).take(indices))

    def copy(self, column_names=None, virtual=True):
        df = geovaex.from_df(df=self, geometry=self.geometry.get_raw_geometry(), crs=self.geometry.crs)
        i1, i2 = self.get_active_range()
        df.geometry.set_active_range(i1, i2)
        return df

    def trim(self, inplace=False):
        df = super(GeoDataFrame, self).trim(inplace=inplace)
        df._geoseries = df.geometry.trim(inplace=inplace)
        return df

    def convex_hull(self, inplace=False):
        if inplace:
            self.geoseries = self.geometry.convex_hull()
        else:
            return geovaex.from_df(df=self.trim(), geometry=self.geometry.convex_hull().get_raw_geometry(), crs=self.geometry.crs)
