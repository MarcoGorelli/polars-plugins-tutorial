import polars as pl
from polars.utils.udfs import _get_shared_lib_location
from polars.type_aliases import IntoExpr
from minimal_plugin.utils import parse_into_expr

lib = _get_shared_lib_location(__file__)


<<<<<<< HEAD
def noop(expr: str | pl.Expr) -> pl.Expr:
    if isinstance(expr, str):
        expr = pl.col(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="noop",
        is_elementwise=True,
    )

def abs_i64(expr: str | pl.Expr) -> pl.Expr:
    if isinstance(expr, str):
        expr = pl.col(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="abs_i64",
        is_elementwise=True,
    )

def abs_numeric(expr: str | pl.Expr) -> pl.Expr:
    if isinstance(expr, str):
        expr = pl.col(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="abs_numeric",
        is_elementwise=True,
    )

def sum_i64(expr: str | pl.Expr, other: IntoExpr) -> pl.Expr:
    if isinstance(expr, str):
        expr = pl.col(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="sum_i64",
        is_elementwise=True,
        args=[other]
    )

def cum_sum(expr: str | pl.Expr) -> pl.Expr:
    if isinstance(expr, str):
        expr = pl.col(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="cum_sum",
        is_elementwise=False,
    )

def pig_latinnify(expr: str | pl.Expr) -> pl.Expr:
    if isinstance(expr, str):
        expr = pl.col(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="pig_latinnify",
        is_elementwise=True,
    )

def abs_i64_fast(expr: str | pl.Expr) -> pl.Expr:
    if isinstance(expr, str):
        expr = pl.col(expr)
=======

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
>>>>>>> origin/main
    return expr.register_plugin(
        lib=lib,
        symbol="abs_i64_fast",
        is_elementwise=True,
    )
    
<<<<<<< HEAD
def add_suffix(expr: str | pl.Expr, *, suffix: str) -> pl.Expr:
    if isinstance(expr, str):
        expr = pl.col(expr)
=======
def add_suffix(expr: IntoExpr, *, suffix: str) -> pl.Expr:
    expr = parse_into_expr(expr)
>>>>>>> origin/main
    return expr.register_plugin(
        lib=lib,
        symbol="add_suffix",
        is_elementwise=True,
        kwargs={"suffix": suffix}
    )

<<<<<<< HEAD
def snowball_stem(expr: str | pl.Expr) -> pl.Expr:
    if isinstance(expr, str):
        expr = pl.col(expr)
=======
def snowball_stem(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
>>>>>>> origin/main
    return expr.register_plugin(
        lib=lib,
        symbol="snowball_stem",
        is_elementwise=True,
    )

<<<<<<< HEAD
def weighted_mean(expr: str | pl.Expr, weights: IntoExpr) -> pl.Expr:
    if isinstance(expr, str):
        expr = pl.col(expr)
=======
def weighted_mean(expr: IntoExpr, weights: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
>>>>>>> origin/main
    return expr.register_plugin(
        lib=lib,
        symbol="weighted_mean",
        is_elementwise=True,
        args=[weights]
    )
<<<<<<< HEAD

def shift_struct(expr: str | pl.Expr) -> pl.Expr:
    if isinstance(expr, str):
        expr = pl.col(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="shift_struct",
        is_elementwise=True,
    )
=======
>>>>>>> origin/main
