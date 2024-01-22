import polars as pl
from polars.utils.udfs import _get_shared_lib_location
from polars.type_aliases import IntoExpr

lib = _get_shared_lib_location(__file__)


def noop(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="noop",
        is_elementwise=True,
    )

def abs_i64(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="abs_i64",
        is_elementwise=True,
    )

def abs_numeric(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="abs_numeric",
        is_elementwise=True,
    )

def sum_i64(expr: IntoExpr, other: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="sum_i64",
        is_elementwise=True,
        args=[other]
    )

def cum_sum(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="cum_sum",
        is_elementwise=False,
    )

def pig_latinnify(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="pig_latinnify",
        is_elementwise=True,
    )

def abs_i64_fast(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="abs_i64_fast",
        is_elementwise=True,
    )
    
def add_suffix(expr: IntoExpr, *, suffix: str) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="add_suffix",
        is_elementwise=True,
        kwargs={"suffix": suffix}
    )

def snowball_stem(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="snowball_stem",
        is_elementwise=True,
    )

def weighted_mean(expr: IntoExpr, weights: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="weighted_mean",
        is_elementwise=True,
        args=[weights]
    )
