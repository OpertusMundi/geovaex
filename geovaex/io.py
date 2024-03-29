from random import sample
from osgeo import ogr, osr, gdal
import os
import sys
import pyarrow as pa
from pyarrow import csv
import pygeos as pg
import numpy as np
import pandas as pd
import warnings
from ._version import __version__

NUMBER_OF_SAMPLES = 1000
IS_GEOM_THRESHOLD = 0.9
SET_OF_ACCEPTABLE_SHAPES = ['point', 'linestring', 'multipoint', 'multilinestring',
                            'polygon', 'multipolygon', 'geometrycollection', 'linearring']


def to_arrow_table(file, chunksize=2000000, crs=None, encoding='utf8', lat=None, lon=None, geom=None, **kwargs):
    """Reads a file to an arrow table.
    It reads a file in batches and yields a pyarrow table. The size of each chunk is determined
    either by the parameter ``chunksize`` in case of geospatial files which represents number of
    features either by the parameter ``block_size`` in case of CSV files which is measured in bytes.
    Parameters:
        file (string): The full path of the input file.
        chunksize (int): The number of features of each chunk (does not apply in CSV; default: 2000000).
        crs (string): The dataset native crs (default: read from file).
        encoding (string): File encoding.
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
        dataSource = gdal.OpenEx(file, open_options=[f'ENCODING={encoding}'])
    except:
        print(sys.exc_info()[1])
    else:
        extension = (os.path.splitext(file)[1]).split('.')
        extension = extension[len(extension) - 1]
        driver = dataSource.GetDriver().ShortName if dataSource is not None else extension.upper()
        metadata = {'source file': filename, 'driver': driver, 'geovaex version': __version__}
        if driver == 'CSV' or driver == 'TSV':
            delimiter = kwargs.pop('delimiter', ',')
            if extension.lower() == 'tsv':
                delimiter = "\t"
            for table in _csv_to_table(file, metadata=metadata, lat=lat, lon=lon, geom=geom, crs=crs, encoding=encoding, delimiter=delimiter, **kwargs):
                yield table
        elif driver == 'netCDF':
            raise Exception('NetCDF files are not yet supported by geovaex.')
        else:
            if dataSource is None:
                raise FileNotFoundError('ERROR: Could not open %s.' % (file))
            print('Opened file %s, using driver %s.' % (filename, dataSource.GetDriver().ShortName))
            for table in _datasource_to_table(dataSource, metadata=metadata, chunksize=chunksize, crs=crs):
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
        warnings.warn(f'Given CRS {crs} different from native CRS {native_crs}.')
    length = layer.GetFeatureCount()
    print(f'Found {length} features.')

    metadata['layer'] = layer.GetName()
    lower = 0
    for i in range(1, length//chunksize + 2):
        upper = min(i*chunksize, length)
        table = _export_table(layer, crs, lower, upper, metadata=metadata)
        lower = upper
        yield table


def _csv_to_table(file, metadata=None, lat=None, lon=None, geom=None, crs=None, **kwargs):
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
    elif geom is not None:
        type_of_geom = 'wkt'
    else:
        type_of_geom = None
    batches = csv.open_csv(file, read_options=read_options, parse_options=parse_options, convert_options=convert_options)
    if type_of_geom is None:
        type_of_geom, geom, lat, lon = _get_geom_info(batches.schema.names, file, parse_options.delimiter)
    print('Opened file %s, using pyarrow CSV reader.' % (os.path.basename(file)))

    eof = False
    while not eof:
        try:
            batch = batches.read_next_batch()
        except StopIteration:
            eof = True
        else:
            table = pa.Table.from_batches([batch])
            try:
                if type_of_geom == 'latlon':
                    table = _geometry_from_latlon(table, lat, lon, crs=crs)
                else:
                    table = _geometry_from_wkt(table, geom, crs=crs)
            # Not spatial file
            except TypeError:
                pass
            except KeyError:
                pass
            else:
                table = table.replace_schema_metadata(metadata=metadata)
            yield table


def _get_geom_info(schema, file, delimiter):
    """Get geometry info for CSV file according to GeoCSV specification.

    See also https://giswiki.hsr.ch/GeoCSV.

    Parameters:
        schema (list): List of column names.
        file (string): The full path of the input file.
        delimiter (string): The delimiter of the csv file
    Returns:
        (tuple)
    """
    lat, lon, geom, type_of_geom = None, None, None, None
    if 'wkt' in schema:
        geom = 'wkt'
        type_of_geom = 'wkt'
    elif 'WKT' in schema:
        geom = 'WKT'
        type_of_geom = 'wkt'
    elif 'geometry' in schema:
        geom = 'geometry'
        type_of_geom = 'wkt'
    elif 'GEOMETRY' in schema:
        geom = 'GEOMETRY'
        type_of_geom = 'wkt'
    elif 'longitude' in schema and 'latitude' in schema:
        lat = 'latitude'
        lon = 'longitude'
        type_of_geom = 'latlon'
    elif 'LONGITUDE' in schema and 'LATITUDE' in schema:
        lat = 'LATITUDE'
        lon = 'LONGITUDE'
        type_of_geom = 'latlon'
    elif 'lon' in schema and 'lat' in schema:
        lat = 'lat'
        lon = 'lon'
        type_of_geom = 'latlon'
    elif 'LON' in schema and 'LAT' in schema:
        lat = 'LAT'
        lon = 'LON'
        type_of_geom = 'latlon'
    elif 'long' in schema and 'lat' in schema:
        lat = 'lat'
        lon = 'long'
        type_of_geom = 'latlon'
    elif 'LONG' in schema and 'LAT' in schema:
        lat = 'LAT'
        lon = 'LONG'
        type_of_geom = 'latlon'
    elif 'x' in schema and 'y' in schema:
        lat = 'y'
        lon = 'x'
        type_of_geom = 'latlon'
    elif 'X' in schema and 'Y' in schema:
        lat = 'Y'
        lon = 'X'
        type_of_geom = 'latlon'
    else:
        detected_wkt_geom_col = _find_csv_geom_column(schema, file, delimiter)
        if detected_wkt_geom_col is not None:
            geom = detected_wkt_geom_col
            type_of_geom = 'wkt'
    return type_of_geom, geom, lat, lon


def _find_csv_geom_column(schema, file: str, delimiter):
    """Detect the name of the column containing the geometric information"""

    for column in schema:
        column_data = pd.read_csv(file, usecols=[column], sep=delimiter)
        if _is_geom(column_data.iloc[:, 0]):
            return column
    return None


def _is_geom(column_data):
    if column_data.dtype != 'object':
        return False
    try:
        matches = 0
        materialized_column_data = list(column_data)
        for row in sample(materialized_column_data, min(NUMBER_OF_SAMPLES, len(materialized_column_data))):
            is_shape = row.strip().lower().split('(')[0].strip() in SET_OF_ACCEPTABLE_SHAPES
            if row.strip().endswith(')') and is_shape:
                matches += 1
        return matches > IS_GEOM_THRESHOLD * NUMBER_OF_SAMPLES
    except AttributeError:
        return False


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
        # by default assume utf-8 encoding
        if 'encoding' not in kwargs:
            encoding = 'utf-8'
            if not os.path.isfile(file):
                cpg_file = [os.path.join(file, f) for f in os.listdir(file) if f.endswith('.cpg')]
                if len(cpg_file) == 1:
                    # if there is a cpg file read the encoding out of its first line
                    cpg_file = cpg_file[0]
                    with open(cpg_file) as f:
                        encoding = f.readline()
            arrow_generator = to_arrow_table(file, chunksize=chunksize, crs=crs, encoding=encoding, **kwargs)
        else:
            arrow_generator = to_arrow_table(file, chunksize=chunksize, crs=crs, **kwargs)

        for table in arrow_generator:
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

    sep = kwargs.pop('delimiter', ',')

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

        chunk_pdf.to_csv(path_or_buf=path, mode=mode, header=header, sep=sep, index=False, **kwargs)


def export_spatial(gdf, path, driver=None, column_names=None, selection=False, virtual=True, chunksize=1000000, **kwargs):
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
    if len(gdf) > 1000:
        sample = gdf.sample(n=1000)
    else:
        sample = gdf
    types = np.unique(pg.get_type_id(sample.geometry))
    if len(types) == 2:
        if 0 in types and 4 in types:
            types = [4]
        elif 1 in types and 5 in types:
            types = [5]
        elif 3 in types and 6 in types:
            types = [6]
    if (len(types) != 1):
        raise Exception('ERROR: Could not write multiple geometries to %s.' % (driver))
    try:
        layer = ds.CreateLayer(gdf.metadata['layer'], srs, geometric_types[types[0]])
    except KeyError:
        layer = ds.CreateLayer((os.path.splitext(path)[1]).split('.')[0], srs, geometric_types[types[0]])
    if layer is None:
        raise Exception('ERROR: Cannot write layer, file extension not consistent with driver, or geometric type incompatible with driver.')
    fields = _get_datatypes(gdf, column_names=column_names, virtual=virtual)
    for field_name in fields:
        key = 'str' if fields[field_name] == 'object' else fields[field_name][0:3]
        field = ogr.FieldDefn(field_name, field_types[key])
        if key == 'str':
            field.SetWidth(254)
        elif key == 'flo':
            field.SetWidth(254)
            field.SetPrecision(10)
        layer.CreateField(field)

    geom_arr = gdf.geometry._geometry
    if selection not in [None, False] or gdf.filtered:
        mask = gdf.evaluate_selection_mask(selection)
        geom_arr = geom_arr.filter(mask)

    for i1, i2, chunk in gdf.evaluate_iterator(list(fields.keys()), selection=selection, chunk_size=chunksize):
        geom_chunk = geom_arr[i1:i2]
        for i in range(len(chunk[0])):
            feature = ogr.Feature(layer.GetLayerDefn())
            for field_i, field in enumerate(fields):
                value = chunk[field_i][i]
                try:
                    converted_value = value.item()
                except AttributeError:
                    converted_value = value
                feature.SetField(field, converted_value)
            geometry = geom_chunk[i]
            geometry = ogr.CreateGeometryFromWkb(geometry.as_py() if isinstance(geometry, pa.lib.BinaryScalar) else geometry)
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
    if crs is None:
        field = pa.field('geometry', pa.binary())
    else:
        field = pa.field('geometry', pa.binary(), metadata={'crs': crs})
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
    if geom == 'geometry':
        column_names = table.column_names
        column_names[column_names.index('geometry')] = 'geometry_'
        table = table.rename_columns(column_names)
        geom = 'geometry_'
    geometry = pg.to_wkb(pg.from_wkt(table.column(geom)))
    if crs is None:
        crs = 'EPSG:4326'
    field = pa.field('geometry', pa.binary(), metadata={'crs': crs})
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
        autogenerate_column_names=kwargs.pop('autogenerate_column_names', False),
        encoding=kwargs.pop('encoding', 'utf8'),
    )


def _convert_options_from_dict(**kwargs):
    """Returns the convert options for CSV.
    Returns:
        (object) A pyarrow ConvertOptions object.
    """
    return csv.ConvertOptions(
        check_utf8=kwargs.pop('check_utf8', True),
        column_types=kwargs.pop('column_types', None),
        null_values=kwargs.pop('null_values', [" "]),
        true_values=kwargs.pop('true_values', None),
        false_values=kwargs.pop('false_values', None),
        strings_can_be_null=kwargs.pop('strings_can_be_null', True),
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

    features = [layer.GetNextFeature() for i in range(lower, upper)]
    geometry = pa.array(feature.GetGeometryRef().ExportToWkb() if feature.GetGeometryRef() is not None else None for feature in features if feature is not None)
    arrow_arrays.append(geometry)
    fields = [pa.field('geometry', pa.binary(), metadata={'crs': crs})] if crs is not None else [pa.field('geometry', pa.binary())]
    for column_name in column_names:
        if column_name == 'geometry':
            continue
        arr = pa.array(feature.GetField(column_name) for feature in features if feature is not None)
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
