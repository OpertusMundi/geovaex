"""Patches to important vaex current version bugs from master."""
import vaex
import numpy as np
import ast
import _ast
import six
import difflib

def _get_column_index_item(self, slice):
    start, stop, step = slice.start, slice.stop, slice.step
    start = start or 0
    stop = stop or len(self)
    assert step in [None, 1]
    indices = self.indices[start:stop]
    ar_unfiltered = self.df.columns[self.name]
    if self.masked:
        mask = indices == -1
    if isinstance(ar_unfiltered, vaex.column.Column):
        # TODO: this is a workaround, since we do not require yet
        # that Column classes knovaexw how to deal with indices, we get
        # the minimal slice, and get those (not the most efficient)
        if self.masked:
            unmasked_indices = indices[~mask]
            if len(unmasked_indices) > 0:
                i1, i2 = np.min(unmasked_indices), np.max(unmasked_indices)
            else:
                i1, i2 = 0, 0
        else:
            i1, i2 = np.min(indices), np.max(indices)
        ar_unfiltered = ar_unfiltered[i1:i2+1]
        if self.masked:
            indices = indices - i1
        else:
            indices = indices - i1
    take_indices = indices
    if self.masked:
        # arrow and numpy do not like the negative indices, so we set them to 0
        take_indices = indices.copy()
        take_indices[mask] = 0
    ar = ar_unfiltered[take_indices]
    assert not np.ma.isMaskedArray(indices)
    if self.masked:
        # TODO: we probably want to keep this as arrow array if it originally was
        return np.ma.array(ar, mask=mask)
    else:
        return ar

vaex.column.ColumnIndexed.__getitem__ = _get_column_index_item


# allow unicode variable names in expressom, fixes #949 (#0470ff6)
def _validate_expression(expr, variable_set, function_set=[], names=None):
    from vaex.expresso import ast_Num, ast_Str, last_func, validate_func
    global last_func
    names = names if names is not None else []
    if isinstance(expr, six.string_types):
        node = ast.parse(expr)
        if len(node.body) != 1:
            raise ValueError("expected one expression, got %r" %
                             len(node.body))
        first_expr = node.body[0]
        if not isinstance(first_expr, _ast.Expr):
            raise ValueError("expected an expression got a %r" %
                             type(node.body))
        _validate_expression(first_expr.value, variable_set,
                            function_set, names)
    elif isinstance(expr, _ast.BinOp):
        if expr.op.__class__ in valid_binary_operators:
            _validate_expression(expr.right, variable_set, function_set, names)
            _validate_expression(expr.left, variable_set, function_set, names)
        else:
            raise ValueError("Binary operator not allowed: %r" % expr.op)
    elif isinstance(expr, _ast.UnaryOp):
        if expr.op.__class__ in valid_unary_operators:
            _validate_expression(expr.operand, variable_set,
                                function_set, names)
        else:
            raise ValueError("Unary operator not allowed: %r" % expr.op)
    elif isinstance(expr, _ast.Name):
        # validate_id(expr.id) Remove validation
        if expr.id not in variable_set:
            matches = difflib.get_close_matches(expr.id, list(variable_set))
            msg = "Column or variable %r does not exist." % expr.id
            if matches:
                msg += ' Did you mean: ' + " or ".join(map(repr, matches))

            raise NameError(msg)
        names.append(expr.id)
    elif isinstance(expr, ast_Num):
        pass  # numbers are fine
    elif isinstance(expr, ast_Str):
        pass  # as well as strings
    elif isinstance(expr, _ast.Call):
        validate_func(expr.func, function_set)
        last_func = expr
        for arg in expr.args:
            _validate_expression(arg, variable_set, function_set, names)
        for arg in expr.keywords:
            _validate_expression(arg, variable_set, function_set, names)
    elif isinstance(expr, _ast.Compare):
        _validate_expression(expr.left, variable_set, function_set, names)
        for op in expr.ops:
            if op.__class__ not in valid_compare_operators:
                raise ValueError("Compare operator not allowed: %r" % op)
        for comparator in expr.comparators:
            _validate_expression(comparator, variable_set, function_set, names)
    elif isinstance(expr, _ast.keyword):
        _validate_expression(expr.value, variable_set, function_set, names)
    elif isinstance(expr, _ast.Subscript):
        _validate_expression(expr.value, variable_set, function_set, names)
        if isinstance(expr.slice.value, _ast.Num):
            pass  # numbers are fine
        else:
            raise ValueError(
                "Only subscript/slices with numbers allowed, not: %r" % expr.slice.value)
    else:
        last_func = expr
        raise ValueError("Unknown expression type: %r" % type(expr))

vaex.expresso.validate_expression = _validate_expression
