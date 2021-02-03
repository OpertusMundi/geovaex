import sys, os
try:
    from osgeo import ogr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')
import pyarrow as pa
import numpy as np
import collections
from vaex import utils, superutils, from_arrow_table
from vaex.dataframe import DataFrameConcatenated
from vaex.column import ColumnSparse
from vaex_arrow.dataset import DatasetArrow
import geovaex.io
from geovaex.geodataframe import GeoDataFrame
import warnings
from ._version import __version_tuple__, __version__


def custom_formatwarning(msg, *args, **kwargs):
    """Ignore everything except the message."""
    return str(msg) + '\n'
warnings.formatwarning = custom_formatwarning


def open(path):
    """Opens an arrow spatial file.
    Parameters:
        path (string): The file's full path.
    Returns:
        (object) A GeoDataFrame object.
    """
    source = pa.memory_map(path)
    try:
        # first we try if it opens as stream
        reader = pa.ipc.open_stream(source)
    except pa.lib.ArrowInvalid:
        # if not, we open as file
        reader = pa.ipc.open_file(source)
        # for some reason this reader is not iterable
        batches = [reader.get_batch(i) for i in range(reader.num_record_batches)]
    else:
        # if a stream, we're good
        batches = reader  # this reader is iterable
    table = pa.Table.from_batches(batches)
    if table.schema.metadata is not None and b'geovaex version' in table.schema.metadata.keys():
        metadata = table.schema.metadata
        print('Opened file %s, created by geovaex v%s using %s driver.' % (os.path.basename(path), metadata[b'geovaex version'].decode(), metadata[b'driver'].decode()))
        df = from_arrow_spatial_table(table)
    else:
        warnings.warn('Not a spatial arrow file. Returning a Vaex DataFrame.')
        df = from_arrow_table(table).copy()
    return df


def read_file(path, convert=True, **kwargs):
    """Reads a generic spatial file.
    Parameters:
        path (string): The spatial file full path.
        convert (bool|string): Exports to arrow file when convert is a path. If True,
            ``arrow_path = path+'.arrow'``.
        **kwargs: Extra keyword arguments.
    Returns:
        (object) A GeoDataFrame object.
    """
    if convert == False:
        table = pa.concat_tables(geovaex.io.to_arrow_table(path, **kwargs), promote=False)
        if table.schema.metadata is not None and b'geovaex version' in table.schema.metadata.keys():
            df = from_arrow_spatial_table(table)
        else:
            warnings.warn('Not a spatial file. Returning a Vaex DataFrame.')
            df = from_arrow_table(table).copy()
        return df

    arrow_file = os.path.splitext(path)[0] + '.arrow' if convert == True else convert
    to_arrow(path, arrow_file, **kwargs)
    return open(arrow_file)


def to_arrow(file, arrow_file, chunksize=2000000, crs=None, **kwargs):
    """ Alias to geovaex.io.to_arrow. """
    return geovaex.io.to_arrow(file, arrow_file, chunksize=chunksize, crs=crs, **kwargs)


def from_df(df, geometry, crs=None, metadata=None, column_names=None, virtual=True):
    """Creates a GeoDataFrame from a vaex DataFrame
    Parameters:
        df (object): The vaex DataFrame.
        geometry (object): A GeoSeries object or geometry column.
        crs (string): The CRS of geometry (optional).
        metadata (dict): GeoDataFrame metadata (optional).
    Returns:
        (object) A GeoDataFrame object.
    """
    copy = GeoDataFrame(geometry=geometry, crs=crs, metadata=metadata)
    copy._length_unfiltered = df._length_unfiltered
    copy._length_original = df._length_original
    copy._cached_filtered_length = df._cached_filtered_length
    copy._index_end = df._index_end
    copy._index_start = df._index_start
    copy._active_fraction = df._active_fraction
    copy._renamed_columns = list(df._renamed_columns)
    copy.units.update(df.units)
    copy.variables.update(df.variables)  # we add all, could maybe only copy used
    copy._categories.update(df._categories)
    if column_names is None:
        column_names = df.get_column_names(hidden=True, alias=False)
        copy._column_aliases = dict(df._column_aliases)
    else:
        copy._column_aliases = {alias: real_name for alias, real_name in self._column_aliases.items()}
        column_names = [df._column_aliases.get(k, k) for k in column_names]
    copy._column_aliases = dict(df._column_aliases)

    copy.functions.update(df.functions)
    for key, value in df.selection_histories.items():
        if df.get_selection(key):
            copy.selection_histories[key] = list(value)
            if key == '__filter__':
                copy._selection_masks[key] = df._selection_masks[key]
            else:
                copy._selection_masks[key] = superutils.Mask(copy._length_original)
            np.asarray(copy._selection_masks[key])[:] = np.asarray(df._selection_masks[key])
    for key, value in df.selection_history_indices.items():
        if df.get_selection(key):
            copy.selection_history_indices[key] = value
            copy._selection_mask_caches[key] = collections.defaultdict(dict)
            copy._selection_mask_caches[key].update(df._selection_mask_caches[key])

    depending = set()
    added = set()
    for name in column_names:
        added.add(name)
        if name in df.columns:
            column = df.columns[name]
            copy.add_column(name, column, dtype=df._dtypes_override.get(name))
            if isinstance(column, ColumnSparse):
                copy._sparse_matrices[name] = df._sparse_matrices[name]
        elif name in df.virtual_columns:
            if virtual:
                copy.add_virtual_column(name, df.virtual_columns[name])
                deps = [key for key, value in copy._virtual_expressions[name].ast_names.items()]
                depending.update(deps)
        else:
            real_column_name = copy._column_aliases.get(name, name)
            valid_name = utils.find_valid_name(name)
            df.validate_expression(real_column_name)
            copy[valid_name] = copy._expr(real_column_name)
            deps = [key for key, value in copy._virtual_expressions[valid_name].ast_names.items()]
            depending.update(deps)
    if df.filtered:
        selection = df.get_selection('__filter__')
        depending |= selection._depending_columns(df)
    depending.difference_update(added)

    hide = []

    while depending:
        new_depending = set()
        for name in depending:
            added.add(name)
            if name in df.columns:
                copy.add_column(name, df.columns[name], dtype=df._dtypes_override.get(name))
                hide.append(name)
            elif name in df.virtual_columns:
                if virtual:
                    copy.add_virtual_column(name, df.virtual_columns[name])
                    deps = [key for key, value in df._virtual_expressions[name].ast_names.items()]
                    new_depending.update(deps)
                hide.append(name)

        new_depending.difference_update(added)
        depending = new_depending
    for name in hide:
        copy._hide_column(name)

    copy.copy_metadata(df)
    return copy


def from_arrow_spatial_table(table):
    """Constructs a GeoDataFrame using an arrow spatial table.
    Parameters:
        table (object): An arrow table.
    Returns:
        (object) The geovaex DataFrame.
    """
    try:
        num_chunks = table.column('geometry').num_chunks
    except:
        raise Exception('ERROR: Geometry not found in file.')
    # Geometry
    geometry = table.column('geometry')
    try:
        crs = table.schema.field('geometry').metadata[b'crs'].decode()
    except:
        crs = None
    # Vaex dataframe
    if num_chunks > 1:
        dataframes = [DatasetArrow(table=t) for t, chunk in _split_table(table, num_chunks)]
        df = DataFrameConcatenated(dataframes)
    else:
        df = _create_df(table)
    return from_df(df=df, geometry=geometry, crs=crs, metadata=table.schema.metadata)


def _split_table(table, num_chunks):
    """Splits an arrow table into chunks.
    Parameters:
        table (object): The arrow table.
        num_chunks (int): The number of chunks.
    Yields:
        (object): The next arrow table chunk.
    """
    new_schema = pa.schema([s for s in table.schema if s.name != 'geometry'])
    for chunk in range(num_chunks):
        pa_arrays = [table.column(entry.name).chunk(chunk) for entry in new_schema]
        yield (pa.Table.from_arrays(pa_arrays, schema=new_schema), chunk)


def _create_df(table):
    """Creates a vaex DataFrame from an arrow table.
    Parameters:
        table (object): The arrow table.
    Returns:
        (object) The vaex DataFrame.
    """
    # TODO Better way to handle NULL types
    new_schema = pa.schema([s for s in table.schema if s.name != 'geometry' and s.type.id != 0])
    pa_arrays = [table.column(entry.name).chunk(0) for entry in new_schema]
    t = pa.Table.from_arrays(pa_arrays, schema=new_schema)
    df = DatasetArrow(table=t)
    return df


def gdal_error_handler(err_class, err_num, err_msg):
    errtype = {
        gdal.CE_None:'None',
        gdal.CE_Debug:'Debug',
        gdal.CE_Warning:'Warning',
        gdal.CE_Failure:'Failure',
        gdal.CE_Fatal:'Fatal'
    }
    err_msg = err_msg.replace('\n',' ')
    err_class = errtype.get(err_class, 'None')
    print('%s: %s' % (err_class.upper(), err_msg))
