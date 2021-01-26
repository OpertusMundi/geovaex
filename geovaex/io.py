from osgeo import ogr, osr
import os
import sys
import pyarrow as pa
from pyarrow import csv
import pygeos as pg
import numpy as np
import warnings
from ._version import __version__


def to_arrow_table(file, chunksize=2000000, crs=None, lat=None, lon=None, geom='wkt', **kwargs):
    """Reads a file to an arrow table.
    It reads a file in batches and yields a pyarrow table. The size of each chunk is determined
    either by the parameter ``chunksize`` in case of geospatial files which represents number of
    features either by the parameter ``block_size`` in case of CSV files which is measured in bytes.
    Parameters:
        file (string): The full path of the input file.
        chunksize (int): The number of features of each chunk (does not apply in CSV; default: 2000000).
        crs (string): The dataset native crs (default: read from file).
        lat (string): The column name of latitude (applies only to CSV).
        lon (string): The column name of longitude (applies only to CSV).
        geom (string): The column name of WKT geometry (applies only to CSV).
        **kwargs: Extra keyword arguments used by CSV reader
            (see https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html).
    Yields:
        (object) Arrow table with spatial features.
    """
    filename = os.path.basename(file)
    try:
        dataSource = ogr.Open(file, 0)
    except:
        print(sys.exc_info()[1])
    else:
        driver = dataSource.GetDriver().name if dataSource is not None else (os.path.splitext(file)[1]).split('.')[1].upper()
        metadata = {'source file': filename, 'driver': driver, 'geovaex version': __version__}
        if driver == 'CSV':
            for table in _csv_to_table(file, metadata=metadata, lat=lat, lon=lon, geom=geom, crs=crs, **kwargs):
                yield table
        elif driver == 'netCDF':
            raise Exception('NetCDF files are not yet supported by geovaex.')
        else:
            if dataSource is None:
                raise FileNotFoundError('ERROR: Could not open %s.' % (file))
            print('Opened file %s, using driver %s.' % (filename, dataSource.GetDriver().name))
            for table in _datasource_to_table(dataSource, chunksize=chunksize, crs=crs):
                yield table


def _datasource_to_table(dataSource, metadata={}, chunksize=2000000, crs=None):
    """Transforms a GDAL DataSource to arrow table.
    It reads the dataSource in chunks (with size defined by chunksize) and yields
    an arrow table.
    Parameters:
        dataSource (object): The GDAL input dataSource.
        metadata (dict): Metadata to be written in the arrow table.
        chunksize (int): The chunksize for each table (number of features).
        crs (string): The native CRS of the dataSource (default: read from dataSource)
    Yields:
        (object) Arrow table with spatial features.
    """
    layer = dataSource.GetLayer()
    native_crs = _get_crs(layer)
    crs = crs if crs is not None else native_crs
    if native_crs != crs:
        warnings.warn('Given CRS %s different from native CRS %s.' % (crs, native_crs))
    length = layer.GetFeatureCount()
    print('Found %i features.' % (length))

    metadata['layer'] = layer.GetName()
    lower = 0
    for i in range(1, length//chunksize + 2):
        upper = min(i*chunksize, length)
        table = _export_table(layer, crs, lower, upper, metadata=metadata)
        lower = upper
        yield table


def _csv_to_table(file, metadata=None, lat=None, lon=None, geom='wkt', crs=None, **kwargs):
    """Yields an arrow table from a stream of CSV data.
    Parameters:
        file (string): The full path of the input file.
        metadata (dict): Metadata to be written in the arrow table.
        lat (string): The column name of latitude (applies only to CSV).
        lon (string): The column name of longitude (applies only to CSV).
        geom (string): The column name of WKT geometry (applies only to CSV).
        crs (string): The dataset native crs (default: read from file).
        **kwargs: Extra keyword arguments used by the CSV reader
            (see https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html).
    Yields:
        (object) Arrow table with spatial features.
    """
    parse_options = _parse_options_from_dict(**kwargs)
    read_options = _read_options_from_dict(**kwargs)
    convert_options = _convert_options_from_dict(**kwargs)
    if lat is not None and lon is not None:
        type_of_geom = 'latlon'
    else:
        type_of_geom = 'wkt'
    batches = csv.open_csv(file, read_options=read_options, parse_options=parse_options, convert_options=convert_options)
    print('Opened file %s, using pyarrow CSV reader.' % (os.path.basename(file)))

    eof = False
    while not eof:
        try:
            batch = batches.read_next_batch()
        except StopIteration:
            eof = True
        else:
            table = pa.Table.from_batches([batch])
            if type_of_geom == 'latlon':
                table = _geometry_from_latlon(table, lat, lon, crs=crs)
            else:
                table = _geometry_from_wkt(table, geom, crs=crs)
            table = table.replace_schema_metadata(metadata=metadata)
            yield table


def to_arrow(file, arrow_file, chunksize=2000000, crs=None, **kwargs):
    """ Converts a spatial vector file into an arrow binary file.
    In case of CSV it uses pyarrow CSV reader, otherwise GDAL is used in order
    to parse the file.
    Parameters:
        file (string): The input file full path.
        arrow_file (string): The full path of output arrow file.
        chunksize (int): The chunksize of the file that is read in each iteration (default: 2000000)
        crs (string): The native CRS of the spatial file (optional)
    """
    with pa.OSFile(arrow_file, 'wb') as sink:
        writer = None
        for table in to_arrow_table(file, chunksize=chunksize, crs=crs, **kwargs):
            b = table.to_batches()
            if writer is None:
                writer = pa.RecordBatchStreamWriter(sink, b[0].schema)
            writer.write_table(table)
    sink.close()


def to_file(gdf, path, column_names=None, selection=False, virtual=True, chunksize=2000000):
    """Saves GeoDataFrame to a file.
    Parameters:
        path (string): The full path of the output file.
        column_names (list): List of column names to wrtie or None for all columns.
        selection (bool): Write selection or not
        virtual (bool): When True, write virtual columns.
        chunksize (int): Chunk size for each write
    """
    metadata = {'source file': '-', 'driver': 'builtin', 'geovaex version': __version__}
    with pa.OSFile(path, 'wb') as sink:
        writer = None
        for i1, i2, table in gdf.to_arrow_table(column_names=column_names, selection=selection, virtual=virtual, chunk_size=chunksize):
            table = table.replace_schema_metadata(metadata=metadata)
            b = table.to_batches()
            if writer is None:
                writer = pa.RecordBatchStreamWriter(sink, b[0].schema)
            writer.write_table(table)
    sink.close()


def export_csv(gdf, path, latlon=False, geom=True, lat_name='lat', lon_name='lot', geom_name='geometry', column_names=None, selection=False, virtual=True, chunksize=1000000, **kwargs):
    """ Writes GeoDataFrame to a CSV spatial file.
    """
    import pandas as pd

    column_names = column_names or gdf.get_column_names(virtual=virtual, strings=True)
    dtypes = gdf[column_names].dtypes
    fields = column_names[:]
    if latlon:
        fields.append(lat_name)
        fields.append(lon_name)
    if geom:
        fields.append(geom_name)

    geom_arr = gdf.geometry._geometry
    if selection not in [None, False] or gdf.filtered:
        mask = gdf.evaluate_selection_mask(selection)
        geom_arr = geom_arr.filter(mask)

    for i1, i2, chunks in gdf.evaluate_iterator(column_names, chunk_size=chunksize, selection=selection):
        if latlon:
            coordinates = pg.get_coordinates(pg.centroid(pg.from_wkb(geom_arr[i1:i2]))).T
            chunks.append(coordinates[0])
            chunks.append(coordinates[1])
        if geom:
            chunks.append(pg.to_wkt(pg.from_wkb(geom_arr[i1:i2])))
        chunk_dict = {col: values for col, values in zip(fields, chunks)}
        chunk_pdf = pd.DataFrame(chunk_dict)

        if i1 == 0:  # Only the 1st chunk should have a header and the rest will be appended
            mode = 'w'
            header = True
        else:
            mode = 'a'
            header = False

        chunk_pdf.to_csv(path_or_buf=path, mode=mode, header=header, index=False, **kwargs)


def export_spatial(gdf, path, driver, column_names=None, selection=False, virtual=True):
    """ Writes a GeoDataFrame into a spatial file.
    Parameters:
        gdf (object): A GeoVaex DataFrame.
        path (string): The full path of the output file.
        driver (string): The driver to be used to convert the DataFrame into a spatial file.
        column_names (list): List of column names to export or None for all columns.
        selection (bool): Export selection or not
        virtual (bool): When True, export virtual columns.
    """
    geometric_types = [ogr.wkbPoint, ogr.wkbLineString, ogr.wkbLinearRing, ogr.wkbPolygon, ogr.wkbMultiPoint, ogr.wkbMultiLineString, ogr.wkbMultiPolygon, ogr.wkbGeometryCollection]
    field_types = {'int': ogr.OFTInteger64, 'str': ogr.OFTString, 'flo': ogr.OFTReal}
    driver = ogr.GetDriverByName(driver) if driver is not None else ogr.GetDriverByName(gdf.metadata['driver'])
    if driver is None:
        raise Exception('ERROR: Driver not supported.')
    ds = driver.CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(gdf.geometry.crs.to_epsg())
    geometries = gdf.geometry.to_pygeos()
    types = np.unique(pg.get_type_id(gdf.geometry[0:1000]))
    if (len(types) != 1):
        raise Exception('ERROR: Could not write multiple geometries to %s.' % (driver))
    try:
        layer = ds.CreateLayer(gdf.metadata['layer'], srs, geometric_types[types[0]])
    except KeyError:
        layer = ds.CreateLayer((os.path.splitext(path)[1]).split('.')[0], srs, geometric_types[types[0]])
    if layer is None:
        raise Exception('ERROR: Cannot write layer, file extension not consistent with driver, or geometric type incompatible with driver.')
    fields = _get_datatypes(gdf, column_names=column_names)
    for field_name in fields:
        field = ogr.FieldDefn(field_name, field_types[fields[field_name][0:3]])
        if fields[field_name] == 'str':
            field.SetWidth(1023)
        layer.CreateField(field)
    items = gdf.to_dict()
    for i in range(len(gdf)):
        feature = ogr.Feature(layer.GetLayerDefn())
        for field in fields:
            feature.SetField(field, items[field][i])
        geometry = gdf.geometry._geometry[i]
        geometry = ogr.CreateGeometryFromWkb(geometry.as_py() if isinstance(geometry, pa.lib.BinaryValue) else geometry)
        feature.SetGeometry(geometry)
        layer.CreateFeature(feature)
        feature = None
    ds = None


def _geometry_from_latlon(table, lat_field, lon_field, crs):
    """Transforms an arrow to table to spatial arrow table, using lat, lon information.
    Extracts the lat, lon information from an arrow table, creates the Point geometry
    and writes the geometry information to the arrow table.
    Parameters:
        table (object): The arrow table.
        lat_field (string): The latitude field name.
        lon_field (string): The longitude field name.
        crs (string): The lat, lon CRS.
    Returns:
        (object): The arrow spatial table.
    """
    lat = table.column(lat_field)
    lon = table.column(lon_field)
    geometry = pg.to_wkb(pg.points(lon, lat))
    field = pa.field('geometry', 'binary', metadata={'crs': crs})
    table = table.append_column(field, [geometry])
    table = table.drop([lat_field, lon_field])
    return table


def _geometry_from_wkt(table, geom, crs):
    """Transforms an arrow to table to spatial arrow table, using geometry information.
    Extracts the geometry information from an arrow table, creates the WKB geometry
    and writes the geometry information to the arrow table.
    Parameters:
        table (object): The arrow table.
        geom (string): The geometry field name.
        crs (string): The lat, lon CRS.
    Returns:
        (object): The arrow spatial table.
    """
    geometry = pg.to_wkb(pg.from_wkt(table.column(geom)))
    field = pa.field('geometry', 'binary', metadata={'crs': crs})
    table = table.append_column(field, [geometry])
    table = table.drop([geom])
    return table


def _parse_options_from_dict(**kwargs):
    """Returns the parse options for CSV.
    Returns:
        (object) A pyarrow ParseOptions object.
    """
    return csv.ParseOptions(
        delimiter=kwargs.pop('delimiter', ','),
        quote_char=kwargs.pop('quote_char', '"'),
        double_quote=kwargs.pop('double_quote', True),
        escape_char=kwargs.pop('escape_char', False),
        newlines_in_values=kwargs.pop('newlines_in_values', False),
        ignore_empty_lines=kwargs.pop('ignore_empty_lines', True)
    )


def _read_options_from_dict(**kwargs):
    """Returns the read options for CSV.
    Returns:
        (object) A pyarrow ReadOptions object.
    """
    return csv.ReadOptions(
        use_threads=kwargs.pop('use_threads', True),
        block_size=kwargs.pop('block_size', 1073741824),
        skip_rows=kwargs.pop('skip_rows', 0),
        column_names=kwargs.pop('column_names', None),
        autogenerate_column_names=kwargs.pop('autogenerate_column_names', False)
    )


def _convert_options_from_dict(**kwargs):
    """Returns the convert options for CSV.
    Returns:
        (object) A pyarrow ConvertOptions object.
    """
    return csv.ConvertOptions(
        check_utf8=kwargs.pop('check_utf8', True),
        column_types=kwargs.pop('column_types', None),
        null_values=kwargs.pop('null_values', None),
        true_values=kwargs.pop('true_values', None),
        false_values=kwargs.pop('false_values', None),
        strings_can_be_null=kwargs.pop('strings_can_be_null', None),
        auto_dict_encode=kwargs.pop('auto_dict_encode', None),
        auto_dict_max_cardinality=kwargs.pop('auto_dict_max_cardinality', None),
        include_columns=kwargs.pop('include_columns', None),
        include_missing_columns=kwargs.pop('include_missing_columns', None)
    )


def _get_datatypes(gdf, column_names=None, virtual=False):
    """Retrieves the datatypes of a GeoDataFrame.
    Parameters:
        gdf (object): The GeoDataFrame object.
        column_names (list): List of column or None for all columns.
        virtual (bool): When True, include virtual columns.
    Returns:
        (dict) A column names - datatype dictionary.
    """
    column_names = column_names or gdf.get_column_names(virtual=virtual, strings=True)
    datatypes = {col: gdf.data_type(col) for col in column_names}
    for col in datatypes:
        try:
            datatypes[col] = datatypes[col].__name__
        except:
            datatypes[col] = datatypes[col].name
    return datatypes


def _export_table(layer, crs, lower, upper, metadata):
    """Exports an arrow table from GDAL layer.
    Parameters:
        layer (object): A GDAL layer object
        crs (string): The native CRS of the layer.
        lower (int): The first feature to read.
        upper (int): The last feature to read.
        metadata (dict): The metadata to be written in the arrow table.
    Returns:
        (object) A pyarrow spatial table
    """
    column_names = _get_layer_definition(layer)
    arrow_arrays = []

    storage = []
    for i in range(lower, upper):
        feature = layer.GetFeature(i)
        if feature is not None:
            geom = feature.GetGeometryRef()
            if geom is not None:
                storage.append(feature.GetGeometryRef().ExportToWkb())
            else:
                storage.append(None)
        else:
            storage.append(None)
    geometry = pa.array(storage)
    arrow_arrays.append(geometry)
    fields = [pa.field('geometry', 'binary', metadata={'crs': crs})]
    for column_name in column_names:
        if column_name == 'geometry':
            continue
        arr = pa.array(layer.GetFeature(i).GetField(column_name) if layer.GetFeature(i) is not None else None for i in range(lower, upper))
        arrow_arrays.append(arr)
        fields.append(pa.field(column_name, arr.type))
    table = pa.Table.from_arrays(arrow_arrays, schema=pa.schema(fields, metadata=metadata))
    return table


def _export_table_from_df(df, geometry_col):
    """Exports a table from a dataframe.
    Parameters:
        df (object): A vaex DataFrame.
        geometry_col (string): The column name containing the geometry.
    Returns:
        (object): An arrow spatial table.
    """
    column_names = df.get_column_names(strings=True)
    arrow_arrays = []

    geometry = pg.from_wkt(df[geometry_col].values)
    geometry = pa.array(pg.to_wkb(geometry))
    arrow_arrays.append(geometry)
    actual_columns = ['geometry']
    for column_name in column_names:
        if column_name == geometry_col:
            continue
        arrow_arrays.append(df[column_name].evaluate())
        actual_columns.append(column_name)
    table = pa.Table.from_arrays(arrow_arrays, actual_columns)
    return table


def _get_crs(layer):
    """Retrieves the CRS of a GDAL layer.
    Parameters:
        layer (object): A GDAL layer object.
    Returns:
        (object) The retrieved CRS
    """
    spatialRef = layer.GetSpatialRef()
    crs = spatialRef.GetName() if spatialRef is not None else None
    return crs


def _get_layer_definition(layer):
    """Retrieves the definition of a GDAL layer.
    Parameters:
        layer (object): A GDAL layer object.
    Returns:
        (list) A list of the fields definitions in the layer.
    """
    schema = []
    ldefn = layer.GetLayerDefn()
    for n in range(ldefn.GetFieldCount()):
        fdefn = ldefn.GetFieldDefn(n)
        schema.append(fdefn.name)
    return schema
