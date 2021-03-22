from . import GeoDataFrame
import pygeos as pg
import numpy as np

def sjoin(left, right, how='left', op="within", distance=None, lprefix='', rprefix='', lsuffix='', rsuffix='', allow_duplication=True):
    """Spatial join.

    Joins two GeoDataFrames on a spatial predicate.

    Arguments:
        left (GeoDataFrame): The left GeoDataFrame.
        right (GeoDataFrame): The right GeoDataFrame.

    Keyword Arguments:
        how (str): how to join, 'left' keeps all rows on the left, and adds columns (with possible missing values)
            'right' is similar with self and other swapped. 'inner' will only return rows which overlap. (default: {'left'})
        op (str): The spatial predicate operation (one of "contains", "within", "intersects", "dwithin"; default: {"within"})
        distance (float): For op="dwithin", the minimum distance between the two geometries (required for 'dwithin'; ignored for other operations; default: {None})
        lprefix (str): prefix to add to the left column names in case of a name collision (default: {''})
        rprefix (str): similar for the right (default: {''})
        lsuffix (str): suffix to add to the left column names in case of a name collision (default: {''})
        rsuffix (str): similar for the right (default: {''})
        allow_duplication (bool): Allow duplication of rows when the joined column contains non-unique values. (default: {True})

    Returns:
        [type] -- [description]
    """
    if not isinstance(left, GeoDataFrame):
        raise ValueError("'left' should be GeoDataFrame, got {}".format(type(left)))

    if not isinstance(right, GeoDataFrame):
        raise ValueError("'right' should be GeoDataFrame, got {}".format(type(right)))
    allowed_ops = ["contains", "within", "intersects", "dwithin"]
    if op not in allowed_ops:
        raise ValueError('`op` "%s" not supported, expected to be one of %s' % (op, allowed_ops))
    allowed_hows = ["left", "right", "inner"]
    if how not in allowed_hows:
        raise ValueError('`how` "%s" not supported, expected to be one of %s' % (how, allowed_hows))

    left = left.copy()

    swapped = False
    if len(left) < len(right):
        swapped = True
        left, right = right, left
        try:
            index = allowed_ops[0:2].index(op)
        except ValueError:
            pass
        else:
            op = allowed_ops[0:2][(1 - index)%2]

    right = right.extract()  # get rid of filters and active_range
    epsg = left.geometry.crs.to_epsg()
    # Reproject if required
    if right.geometry.crs.to_epsg() != epsg:
        right.geometry.to_crs(epsg)
    assert left.length_unfiltered() == left.length_original()

    if op == 'dwithin':
        if distance is None:
            raise ValueError("'distance' is required for operation 'dwithin'")
        right.constructive.buffer(radius=distance, inplace=True)
        op = 'intersects'

    tree_idx = pg.STRtree(right.geometry)
    l_idx, r_idx = tree_idx.query_bulk(left.geometry, predicate=op)
    if len(r_idx) != 0:
        right = right.take(r_idx)
        right.add_column("join_id", l_idx)
    else:
        right.add_column("join_id", np.array([None]*len(right)))
    left.add_column("join_id", np.arange(0, len(left)))

    if swapped:
        left, right = right, left

    left = left.join(right, on="join_id", lprefix=lprefix, rprefix=rprefix, lsuffix=lsuffix, rsuffix=rsuffix, allow_duplication=allow_duplication, how=how)
    left.drop([lprefix + 'join_id' + lsuffix, rprefix + 'join_id' + rsuffix], inplace=True)

    return left
