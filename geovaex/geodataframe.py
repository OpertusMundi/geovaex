from .geoseries import GeoSeries
from vaex.dataframe import DataFrameLocal
import geovaex.io

class GeoDataFrame(DataFrameLocal):
    def __init__(self, geometry, crs=None, path=None, metadata=None):
        super(GeoDataFrame, self).__init__(name=path, path=path, column_names=[])
        self._geoseries = geometry if isinstance(geometry, GeoSeries) else GeoSeries(geometry, crs=crs, df=self)
        self._metadata = metadata

    @property
    def geometry(self):
        return self._geoseries

    @property
    def metadata(self):
        return {key.decode(): self._metadata[key].decode() for key in self._metadata} if self._metadata is not None else None

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
            row = super(GeoDataFrame, self).__getitem__(item)
            row.append(self.geometry[item])
            return row
        return super(GeoDataFrame, self).__getitem__(item)

    def take(self, indices, filtered=True, dropfilter=True):
        df = self.trim()
        geometry = df.geometry.take(indices, filtered=filtered)
        return geovaex.from_df(geometry=geometry, df=super(GeoDataFrame, self).take(indices, filtered=filtered, dropfilter=dropfilter), metadata=self._metadata)

    def copy(self, column_names=None, virtual=True):
        df = geovaex.from_df(df=self, geometry=self.geometry.copy(), metadata=self._metadata)
        df.geometry._df = df
        return df

    def set_active_range(self, i1, i2):
        super(GeoDataFrame, self).set_active_range(i1, i2)
        self.geometry.set_active_range(i1, i2)

    def trim(self, inplace=False):
        df = super(GeoDataFrame, self).trim(inplace=inplace)
        df.geometry.trim(inplace=True)
        return df

    def convex_hull(self, inplace=False):
        if inplace:
            self._geoseries = self.geometry.convex_hull()
        else:
            return geovaex.from_df(df=self.trim(), geometry=self.geometry.convex_hull(), metadata=self._metadata)

    def centroid(self, inplace=False):
        df = self if inplace else self.trim()
        df._geoseries = df.geometry.centroid()
        return df

    def within(self, geom, chunksize=1000000, max_workers=None):
        filt = self.geometry.within(geom, chunksize=chunksize, max_workers=max_workers)
        df = self.copy()
        df.add_column('tmp', filt, dtype=bool)
        df = df[df.tmp == True]
        df.drop('tmp', inplace=True)
        return df

    def to_geopandas_df(self, column_names=None, selection=None, strings=True, virtual=True, index_name=None, parallel=True, chunk_size=None):
        from shapely.wkb import loads
        import geopandas as gpd
        pd_df = super(GeoDataFrame, self).to_pandas_df(column_names=column_names, selection=selection, strings=strings, virtual=virtual, index_name=index_name, parallel=parallel, chunk_size=chunk_size)
        geometries = self.geometry.to_numpy()
        geometries = [loads(g) for g in geometries]
        return gpd.GeoDataFrame(pd_df, geometry=geometries, crs=self.geometry.crs)

    def to_vaex_df(self):
        return super(GeoDataFrame, self).copy()

    def to_file(self, output_file, driver=None):
        return geovaex.io.to_file(self, output_file, driver)
