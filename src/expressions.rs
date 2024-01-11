#![allow(clippy::unused_unit)]
use crate::add::impl_add;
use crate::cum_sum::impl_cum_sum;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

fn same_output_type(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    Ok(field.clone())
}

#[polars_expr(output_type_func=same_output_type)]
fn noop(inputs: &[Series]) -> PolarsResult<Series> {
    let s = inputs[0];
    Ok(s.clone())
}

#[polars_expr(output_type=Int64)]
fn add(inputs: &[Series]) -> PolarsResult<Series> {
    let left = inputs[0].i64()?;
    let right = inputs[1].i64()?;
    Ok(impl_add(left, right).into_series())
}

#[polars_expr(output_type=Int64)]
fn cum_sum(inputs: &[Series]) -> PolarsResult<Series> {
    let s = inputs[0].i64()?;
    Ok(impl_cum_sum(s).into_series())
}
