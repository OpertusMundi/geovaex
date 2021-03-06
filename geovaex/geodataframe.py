from .geoseries import GeoSeries
from vaex.dataframe import DataFrameLocal
import geovaex.io
from .operations import constructive
import pyarrow as pa

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
        if isinstance(item, str) and item == 'geometry':
            return self.geometry
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
        """Remained only for backward compatibility. Replaced by constructive.centroid"""
        return self.constructive.centroid()

    def within(self, geom, chunksize=1000000, max_workers=None):
        filt = self.geometry.within(geom, chunksize=chunksize, max_workers=max_workers)
        df = self.copy()
        df.add_column('tmp', filt, dtype=bool)
        df = df[df.tmp == True]
        df.drop('tmp', inplace=True)
        return df

    def to_geopandas_df(self, column_names=None, selection=None, strings=True, virtual=True, index_name=None, parallel=True, chunk_size=None):
        from shapely.wkb import loads
        # TODO: Check if geopandas is installed
        import geopandas as gpd
        pd_df = super(GeoDataFrame, self).to_pandas_df(column_names=column_names, selection=selection, strings=strings, virtual=virtual, index_name=index_name, parallel=parallel, chunk_size=chunk_size)
        geometries = self.geometry.to_numpy()
        geometries = [loads(g) for g in geometries]
        return gpd.GeoDataFrame(pd_df, geometry=geometries, crs=self.geometry.crs)

    def to_vaex_df(self):
        return super(GeoDataFrame, self).copy()

    def to_arrow_table(self, column_names=None, selection=None, strings=True, virtual=True, parallel=True, chunk_size=None):
        from vaex_arrow.convert import arrow_array_from_numpy_array
        has_geometry = column_names is None or 'geometry' in column_names
        if column_names is not None and 'geometry' in column_names:
            column_names.remove('geometry')

        column_names = column_names or self.get_column_names(strings=strings, virtual=virtual)
        if has_geometry:
            geom_arr = self.geometry._geometry
            if selection not in [None, False] or self.filtered:
                mask = self.evaluate_selection_mask(selection)
                geom_arr = geom_arr.filter(mask)

        if chunk_size is not None:
            def iterator():
                for i1, i2, chunks in self.evaluate_iterator(column_names, selection=selection, parallel=parallel, chunk_size=chunk_size):
                    if len(column_names) > 0:
                        chunks = list(map(arrow_array_from_numpy_array, chunks))
                        fields = list(map(lambda chunk_tuple: pa.field(column_names[chunk_tuple[0]], chunk_tuple[1].type), enumerate(chunks)))
                    else:
                        chunks = []
                        fields = []
                    if has_geometry:
                        chunks.append(geom_arr[i1:i2])
                        fields.append(pa.field('geometry', 'binary', metadata={'crs': self.geometry.crs.name}))
                    table = pa.Table.from_arrays(chunks, schema=pa.schema(fields))

                    yield i1, i2, table
            return iterator()
        else:
            if len(column_names) > 0:
                chunks = self.evaluate(column_names, selection=selection, parallel=parallel)
                chunks = list(map(arrow_array_from_numpy_array, chunks))
                fields = list(map(lambda chunk_tuple: pa.field(column_names[chunk_tuple[0]], chunk_tuple[1].type), enumerate(chunks)))
            else:
                chunks = []
                fields = []
            if has_geometry:
                chunks.append(geom_arr)
                fields.append(pa.field('geometry', 'binary', metadata={'crs': self.geometry.crs.name}))
            table = pa.Table.from_arrays(chunks, schema=pa.schema(fields))

            return table

    def to_file(self, path, **kwargs):
        return geovaex.io.to_file(self, path, **kwargs)

    def export_arrow(self, path, **kwargs):
        """Alias to GeoDataFrame.to_file."""
        return self.to_file(path, **kwargs)

    def export(self, path, driver=None, **kwargs):
        """ Writes a GeoDataFrame into a spatial file.
        Parameters:
            path (string): The full path of the output file.
            driver (string): The driver to be used to convert the DataFrame into a spatial file.
            column_names (list): List of column names to export or None for all columns.
            selection (bool): Export selection or not
            virtual (bool): When True, export virtual columns.
        """
        if path.endswith('.csv'):
            driver = 'CSV'
        elif path.endswith('.tsv'):
            driver = 'TSV'
        if driver is not None:
            self.export_spatial(path, driver=driver, **kwargs)
        else:
            try:
                super(GeoDataFrame, self).export(path, **kwargs)
            except ValueError:
                self.export_spatial(path, **kwargs)

    def export_spatial(self, path, driver=None, **kwargs):
        if driver.lower() == 'csv' or driver.lower() == 'tsv':
            delimiter = kwargs.pop('delimiter', ',')
            if driver.lower() == 'tsv':
                delimiter = "\t"
            geovaex.io.export_csv(self, path, delimiter=delimiter, **kwargs)
        else:
            geovaex.io.export_spatial(self, path, driver=driver, **kwargs)

    def export_csv(self, path, **kwargs):
        geovaex.io.export_csv(self, path, **kwargs)

    @property
    def constructive(self):
        return constructive.Constructive(self)
