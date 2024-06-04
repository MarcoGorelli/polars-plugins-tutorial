import polars as pl
from pathlib import Path
from polars.type_aliases import IntoExpr
from minimal_plugin.utils import parse_into_expr
from polars.plugins import register_plugin_function

def noop(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="noop",
        args=[expr],
        is_elementwise=True,
    )

def abs_i64(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="abs_i64",
        args=[expr],
        is_elementwise=True,
    )

def abs_numeric(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="abs_numeric",
        args=[expr],
        is_elementwise=True,
    )

def sum_i64(expr: IntoExpr, other: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="sum_i64",
        args=[expr, other],
        is_elementwise=True,
    )

def cum_sum(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="cum_sum",
        args=[expr],
        is_elementwise=False,
    )

def pig_latinnify(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="pig_latinnify",
        args=[expr],
        is_elementwise=True,
    )

def abs_i64_fast(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="abs_i64_fast",
        args=[expr],
        is_elementwise=True,
    )
    
def add_suffix(expr: IntoExpr, *, suffix: str) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="add_suffix",
        args=[expr],
        is_elementwise=True,
        kwargs={"suffix": suffix}
    )

def snowball_stem(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="snowball_stem",
        args=[expr],
        is_elementwise=True,
    )

def weighted_mean(expr: IntoExpr, weights: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="weighted_mean",
        args=[expr, weights],
        is_elementwise=True,
    )

def shift_struct(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="shift_struct",
        args=[expr],
        is_elementwise=True,
    )


def reverse_geocode(lat: IntoExpr, long: IntoExpr) -> pl.Expr:
    lat = parse_into_expr(lat)
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="reverse_geocode",
        is_elementwise=True,
        args=[lat, long]
    )


def non_zero_indices(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin_function(
        plugin_path=Path(__file__).parent, function_name="non_zero_indices", args=[expr], is_elementwise=True
    )

def vertical_weighted_mean(values: IntoExpr, weights: IntoExpr) -> pl.Expr:
    values = parse_into_expr(values)
    return register_plugin_function(
        plugin_path=Path(__file__).parent, function_name="vertical_weighted_mean",
        args=[values, weights],
        is_elementwise=False, returns_scalar=True
    )
