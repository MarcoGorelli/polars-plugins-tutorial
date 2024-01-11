#![allow(clippy::unused_unit)]
use crate::add::impl_add;
use crate::cum_sum::impl_cum_sum;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use pyo3_polars::export::polars_core::export::num::Signed;
use pyo3_polars::export::polars_core::utils::CustomIterTools;

fn same_output_type(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    Ok(field.clone())
}

#[polars_expr(output_type_func=same_output_type)]
fn noop(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    Ok(s.clone())
}

#[polars_expr(output_type=Int64)]
fn abs_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.i64()?;
    let chunks = ca.downcast_iter().map(|arr| {
        arr.into_iter()
            .map(|opt_v| opt_v.map(|v| v.abs()))
            .collect()
    });
    let out = Int64Chunked::from_chunk_iter(ca.name(), chunks);
    Ok(out.into_series())
}

fn impl_abs_numeric<T>(ca: &ChunkedArray<T>) -> ChunkedArray<T>
where
    T: PolarsNumericType,
    T::Native: Signed,
{
    let chunks = ca.downcast_iter().map(|arr| {
        arr.into_iter()
            .map(|opt_v| opt_v.map(|v| v.abs()))
            .collect()
    });
    ChunkedArray::<T>::from_chunk_iter(ca.name(), chunks)
}

#[polars_expr(output_type_func=same_output_type)]
fn abs_numeric(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    match s.dtype() {
        DataType::Int32 => Ok(impl_abs_numeric(s.i32().unwrap()).into_series()),
        DataType::Int64 => Ok(impl_abs_numeric(s.i64().unwrap()).into_series()),
        DataType::Float32 => Ok(impl_abs_numeric(s.f32().unwrap()).into_series()),
        DataType::Float64 => Ok(impl_abs_numeric(s.f64().unwrap()).into_series()),
        dtype => polars_bail!(InvalidOperation:format!("dtype {dtype} not supported for abs_numeric, expected Int32, Int64, Float32, Float64.")),
    }
}

#[polars_expr(output_type=Int64)]
fn sum_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let left = inputs[0].i64()?;
    let right = inputs[1].i64()?;
    Ok(impl_add(left, right).into_series())
}
