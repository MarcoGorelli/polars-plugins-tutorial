#![allow(clippy::unused_unit)]
use serde::Deserialize;
use polars::prelude::arity::binary_elementwise;
use polars::prelude::*;
// use polars_arrow::array::MutableArray;
// use polars_arrow::array::{MutableUtf8Array, Utf8Array};
use polars_core::utils::align_chunks_binary;
use pyo3_polars::derive::polars_expr;
use pyo3_polars::export::polars_core::export::num::Signed;
use pyo3_polars::export::polars_core::utils::CustomIterTools;
use pyo3_polars::export::polars_core::utils::arrow::array::PrimitiveArray;
// use reverse_geocoder::ReverseGeocoder;

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
        dtype => {
            polars_bail!(InvalidOperation:format!("dtype {dtype} not \
            supported for abs_numeric, expected Int32, Int64, Float32, Float64."))
        }
    }
}

#[polars_expr(output_type=Int64)]
fn sum_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let left = inputs[0].i64()?;
    let right = inputs[1].i64()?;
    let out: Int64Chunked = binary_elementwise(left, right, |left, right| match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        _ => None,
    });
    Ok(out.into_series())
}

#[polars_expr(output_type_func=same_output_type)]
fn cum_sum(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.i64()?;
    let out: Int64Chunked = ca
        .into_iter()
        .scan(None, |state: &mut Option<i64>, x: Option<i64>| {
            let sum = match (*state, x) {
                (Some(state_inner), Some(x)) => {
                    *state = Some(state_inner + x);
                    *state
                }
                (None, Some(x)) => {
                    *state = Some(x);
                    *state
                }
                (_, None) => None,
            };
            Some(sum)
        })
        .collect_trusted();
    let out = out.with_name(ca.name());
    Ok(out.into_series())
}

use std::fmt::Write;

#[polars_expr(output_type=String)]
fn pig_latinnify_1(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;
    let chunks = ca.downcast_iter().map(|arr| {
        arr.into_iter()
            .map(|opt_v| {
                opt_v.map(|value| {
                    // Not the recommended way to do it,
                    // see below for a better way!
                    if let Some(first_char) = value.chars().next() {
                        format!("{}{}ay", &value[1..], first_char)
                    } else {
                        value.to_string()
                    }
                })
            })
            .collect()
    });
    let out = StringChunked::from_chunk_iter(ca.name(), chunks);
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pig_latinnify_2(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out: StringChunked = ca.apply_to_buffer(|value, output| {
        if let Some(first_char) = value.chars().next() {
            write!(output, "{}{}ay", &value[1..], first_char).unwrap()
        }
    });
    Ok(out.into_series())
}

// #[polars_expr(output_type=String)]
// fn reverse_geocode(inputs: &[Series]) -> PolarsResult<Series> {
//     let binding = inputs[0].struct_()?.field_by_name("lat")?;
//     let latitude = binding.f64()?;
//     let binding = inputs[0].struct_()?.field_by_name("lon")?;
//     let longitude = binding.f64()?;
//     let geocoder = ReverseGeocoder::new();
//     let (lhs, rhs) = align_chunks_binary(latitude, longitude);
//     let iter = lhs.downcast_iter().zip(rhs.downcast_iter()).map(
//         |(lhs_arr, rhs_arr)| -> LargeStringArray {
//             let mut buf = String::new();
//             let mut mutarr: MutableUtf8Array<i64> =
//                 MutableUtf8Array::with_capacities(lhs_arr.len(), lhs_arr.len() * 20);

//             for (lhs_opt_val, rhs_opt_val) in lhs_arr.iter().zip(rhs_arr.iter()) {
//                 match (lhs_opt_val, rhs_opt_val) {
//                     (Some(lhs_val), Some(rhs_val)) => {
//                         buf.clear();
//                         let search_result = geocoder.search((*lhs_val, *rhs_val));
//                         write!(buf, "{}", search_result.record.name).unwrap();
//                         mutarr.push(Some(&buf))
//                     }
//                     _ => mutarr.push_null(),
//                 }
//             }
//             let arr: Utf8Array<i64> = mutarr.into();
//             arr
//         },
//     );
//     let out = StringChunked::from_chunk_iter(lhs.name(), iter);
//     Ok(out.into_series())
// }

#[polars_expr(output_type=Int64)]
fn abs_i64_fast(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.i64()?;
    let chunks = ca
        .downcast_iter()
        .map(|arr| arr.values().as_slice())
        .zip(ca.iter_validities())
        .map(|(slice, validity)| {
            let arr: PrimitiveArray<i64> = slice.iter().copied().map(|x| x.abs()).collect_arr();
            arr.with_validity(validity.cloned())
        });
    let out = Int64Chunked::from_chunk_iter(ca.name(), chunks);
    Ok(out.into_series())
}

#[derive(Deserialize)]
struct AddSuffixKwargs {
    suffix: String,
}

#[polars_expr(output_type=String)]
fn add_suffix(inputs: &[Series], kwargs: AddSuffixKwargs) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;
    let out = ca.apply_to_buffer(|value, output| {
        write!(output, "{}{}", value, kwargs.suffix).unwrap();
    });
    Ok(out.into_series())
}
