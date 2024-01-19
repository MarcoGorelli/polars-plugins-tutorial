import polars as pl
from polars.utils.udfs import _get_shared_lib_location
from polars.type_aliases import IntoExpr

lib = _get_shared_lib_location(__file__)


@pl.api.register_expr_namespace("mp")
class MinimalExamples:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def noop(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="noop",
            is_elementwise=True,
        )

    def abs_i64(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="abs_i64",
            is_elementwise=True,
        )

    def abs_numeric(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="abs_numeric",
            is_elementwise=True,
        )

    def sum_i64(self, other: IntoExpr) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="sum_i64",
            is_elementwise=True,
            args=[other]
        )

    def cum_sum(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="cum_sum",
            is_elementwise=False,
        )

    def pig_latinnify_1(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="pig_latinnify_1",
            is_elementwise=True,
        )

    def pig_latinnify_2(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="pig_latinnify_2",
            is_elementwise=True,
        )

    def reverse_geocode(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="reverse_geocode",
            is_elementwise=True,
        )

    def abs_i64_fast(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="abs_i64_fast",
            is_elementwise=True,
        )
    
    def add_suffix(self, *, suffix: str) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="add_suffix",
            is_elementwise=True,
            kwargs={"suffix": suffix}
        )

    def weighted_mean(self, weights: IntoExpr) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="weighted_mean",
            is_elementwise=True,
            args=[weights],
        )
    
    def snowball_stem(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=lib,
            symbol="snowball_stem",
            is_elementwise=True,
        )
