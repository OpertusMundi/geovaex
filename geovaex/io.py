from osgeo import ogr
import os
import contextlib
import sys
import pyarrow as pa
import pygeos as pg
import numpy as np
from geovaex.arrowGeometry import GeometryType

def read_file(file, output='ogr', targetCRS=None, point_cols=None):
    filename = os.path.basename(file)
    try:
        if (point_cols is None):
            dataSource = ogr.Open(file, 0) # 0 means read-only. 1 means writeable.
        else:
            with writeLayerXML(file, point_cols, targetCRS) as temp_file:
                dataSource = ogr.Open(temp_file)
    except:
        print(sys.exc_info()[1]);
    else:
        if dataSource is None:
            print('Could not open %s' % (file))
        else:
            pass
            print('Opened file %s, using driver %s.' % (filename, dataSource.GetDriver().name))

            if (output == 'dataframe'):
                dataSource = to_dataframe(dataSource, targetCRS)
            return dataSource

def to_arrow(input_file, arrow_file, point_cols=None, chunksize=8000000):
    geometry_type = GeometryType()
    pa.register_extension_type(geometry_type)
    path, ext = os.path.splitext(arrow_file)
    ds = read_file(input_file, point_cols=point_cols)
    layer = ds.GetLayer()
    length = layer.GetFeatureCount()
    print('Found %i features.' % (length))
    lower = 0

    with pa.OSFile(arrow_file, 'wb') as sink:
        writer = None
        for i in range(1, length//chunksize + 2):
            upper = min(i*chunksize, length)
            table = _export_table(layer, lower, upper)
            b = table.to_batches()
            if writer is None:
                writer = pa.RecordBatchStreamWriter(sink, b[0].schema)
            writer.write_table(table)
            lower = upper
    sink.close()

def _export_table(layer, lower, upper):
    column_names = getDefinition(layer)
    arrow_arrays = []

    storage = []
    for i in range(lower, upper):
        feature = layer.GetFeature(i)
        storage.append(feature.GetGeometryRef().ExportToWkb())
    storage = pa.array(storage)
    # storage = pa.array(layer.GetFeature(i).GetGeometryRef().ExportToWkb() if layer.GetFeature(i).GetGeometryRef() else None for i in range(lower, upper))
    geometry = pa.ExtensionArray.from_storage(GeometryType(), storage)
    arrow_arrays.append(geometry)
    actual_columns = ['geometry']
    for column_name in column_names:
        if column_name == 'geometry':
            continue
        arrow_arrays.append(pa.array(layer.GetFeature(i).GetField(column_name) for i in range(lower, upper)))
        actual_columns.append(column_name)
    table = pa.Table.from_arrays(arrow_arrays, actual_columns)
    return table

def to_arrow_from_csv(csv, arrow_file, geometry='wkt', point_cols=None, delimiter=','):
    import vaex
    import pyarrow as pa
    df = vaex.from_csv(csv, delimiter=delimiter)
    table = _export_table_from_df(df, geometry)
    b = table.to_batches()
    with pa.OSFile(arrow_file, 'wb') as sink:
        writer = pa.RecordBatchStreamWriter(sink, b[0].schema)
        writer.write_table(table)

def _export_table_from_df(df, geometry_col):
    import pyarrow as pa
    import pygeos as pg
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

def to_dataframe(dataSource, targetCRS=None):

    layer = dataSource.GetLayer()
    del dataSource
    # #create an output datasource in memory
    # outdriver=ogr.GetDriverByName('MEMORY')
    # source=outdriver.CreateDataSource('memData')
    # tmp=outdriver.Open('memData',1)
    # pipes_mem=source.CopyLayer(layer,'temp',['OVERWRITE=YES'])
    # layer=source.GetLayer('temp')

    crs = get_crs(layer)
    crs = crs if crs is not None else targetCRS
    if (crs != targetCRS):
        # TODO Project to targetCRS
        pass
    frame = GeoDataFrame.from_ogr(layer, crs=crs)
    return frame

def get_crs(layer):
    spatialRef = layer.GetSpatialRef()
    crs = "%s:%s" % (spatialRef.GetAttrValue("AUTHORITY", 0), spatialRef.GetAttrValue("AUTHORITY", 1)) if spatialRef is not None else None
    return crs

def getDefinition(layer):
    schema = []
    ldefn = layer.GetLayerDefn()
    for n in range(ldefn.GetFieldCount()):
        fdefn = ldefn.GetFieldDefn(n)
        schema.append(fdefn.name)
    return schema


@contextlib.contextmanager
def writeLayerXML(csv_file, point_cols, crs):
    import tempfile
    name = os.path.basename(csv_file)
    name = os.path.splitext(name)[0]
    xml = '<OGRVRTDataSource><OGRVRTLayer name="%s"><SrcDataSource>%s</SrcDataSource><SrcLayer>%s</SrcLayer><GeometryType>wkbPoint</GeometryType><LayerSRS>%s</LayerSRS><GeometryField encoding="PointFromColumns" x="%s" y="%s"/></OGRVRTLayer></OGRVRTDataSource>'
    try:
        tf = tempfile.NamedTemporaryFile(mode='w+', suffix=".vrt", delete=False)
        filename = tf.name
        tf.write(xml % (name, csv_file, name, crs, point_cols['x'], point_cols['y']))
        tf.close()
        yield filename
    finally:
        os.unlink(filename)
