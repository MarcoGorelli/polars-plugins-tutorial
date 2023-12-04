import polars as pl
from polars.utils.udfs import _get_shared_lib_location
from polars.type_aliases import IntoExpr

lib = _get_shared_lib_location(__file__)


@pl.api.register_expr_namespace("minimal_plugin")
class MinimalExamples:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def noop(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="noop",
            is_elementwise=True,
        )

    def add(self, other: IntoExpr) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="add",
            is_elementwise=True,
            args=[other]
        )
