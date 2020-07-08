import sys
try:
    from osgeo import ogr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')
import geovaex.io
import vaex
import pyarrow as pa
import pygeos as pg
from geovaex.geodataframe import GeoDataFrame
from vaex.dataframe import DataFrameConcatenated
from vaex.column import ColumnSparse
from vaex_arrow.dataset import DatasetArrow

def open(path):
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
    return _load_table(table)

def _split_table(table, num_chunks):
    new_schema = pa.schema([s for s in table.schema if s.name != 'geometry'])
    for chunk in range(num_chunks):
        pa_arrays = [table.column(entry.name).chunk(chunk) for entry in new_schema]
        yield (pa.Table.from_arrays(pa_arrays, schema=new_schema), chunk)

def _load_table(table):
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
    return from_df(df=df, geometry=geometry, crs=crs)


def _create_df(table):
    new_schema = pa.schema([s for s in table.schema if s.name != 'geometry'])
    pa_arrays = [table.column(entry.name).chunk(0) for entry in new_schema]
    t = pa.Table.from_arrays(pa_arrays, schema=new_schema)
    df = DatasetArrow(table=t)
    return df

def from_df(df, geometry, crs=None):
    copy = GeoDataFrame(geometry=geometry, crs=crs)
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
    column_names = df.get_column_names(hidden=True, alias=False)
    copy._column_aliases = dict(df._column_aliases)

    copy.functions.update(df.functions)
    for key, value in df.selection_histories.items():
        if df.get_selection(key):
            copy.selection_histories[key] = list(value)
            if key == FILTER_SELECTION_NAME:
                copy._selection_masks[key] = df._selection_masks[key]
            else:
                copy._selection_masks[key] = vaex.superutils.Mask(copy._length_original)
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
            valid_name = vaex.utils.find_valid_name(name)
            df.validate_expression(real_column_name)
            copy[valid_name] = copy._expr(real_column_name)
            deps = [key for key, value in copy._virtual_expressions[valid_name].ast_names.items()]
            depending.update(deps)
    if df.filtered:
        selection = df.get_selection(FILTER_SELECTION_NAME)
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
