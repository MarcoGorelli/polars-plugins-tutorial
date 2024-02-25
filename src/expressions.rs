#![allow(clippy::unused_unit)]
use polars::prelude::arity::{binary_elementwise, binary_elementwise_values};
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use pyo3_polars::export::polars_core::export::num::Signed;
use pyo3_polars::export::polars_core::utils::arrow::array::PrimitiveArray;
use pyo3_polars::export::polars_core::utils::CustomIterTools;
use serde::Deserialize;

use crate::utils::binary_amortized_elementwise;

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
    let ca: &Int64Chunked = s.i64()?;
    // NOTE: there's a faster way of implementing `abs_i64`, which we'll
    // cover in section 7.
    let out: Int64Chunked = ca.apply(|opt_v: Option<i64>| opt_v.map(|v: i64| v.abs()));
    Ok(out.into_series())
}

fn impl_abs_numeric<T>(ca: &ChunkedArray<T>) -> ChunkedArray<T>
where
    T: PolarsNumericType,
    T::Native: Signed,
{
    ca.apply(|opt_v: Option<T::Native>| opt_v.map(|v: T::Native| v.abs()))
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
    let left: &Int64Chunked = inputs[0].i64()?;
    let right: &Int64Chunked = inputs[1].i64()?;
    // Note: there's a faster way of summing two columns, see
    // section 7.
    let out: Int64Chunked = binary_elementwise(
        left,
        right,
        |left: Option<i64>, right: Option<i64>| match (left, right) {
            (Some(left), Some(right)) => Some(left + right),
            _ => None,
        },
    );
    Ok(out.into_series())
}

#[polars_expr(output_type_func=same_output_type)]
fn cum_sum(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca: &Int64Chunked = s.i64()?;
    let out: Int64Chunked = ca
        .into_iter()
        .scan(0_i64, |state: &mut i64, x: Option<i64>| match x {
            Some(x) => {
                *state += x;
                Some(Some(*state))
            }
            None => Some(None),
        })
        .collect_trusted();
    let out: Int64Chunked = out.with_name(ca.name());
    Ok(out.into_series())
}

use std::borrow::Cow;
use std::fmt::Write;

#[polars_expr(output_type=String)]
fn pig_latinnify(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;
    let out: StringChunked = ca.apply(|opt_v: Option<&str>| {
        opt_v.map(|value: &str| {
            // Not the recommended way to do it,
            // see below for a better way!
            if let Some(first_char) = value.chars().next() {
                Cow::Owned(format!("{}{}ay", &value[1..], first_char))
            } else {
                Cow::Owned(value.to_string())
            }
        })
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

// use rust_stemmers::{Algorithm, Stemmer};

// #[polars_expr(output_type=String)]
// fn snowball_stem(inputs: &[Series]) -> PolarsResult<Series> {
//     let ca: &StringChunked = inputs[0].str()?;
//     let en_stemmer = Stemmer::create(Algorithm::English);
//     let out: StringChunked = ca.apply_to_buffer(|value: &str, output: &mut String| {
//         write!(output, "{}", en_stemmer.stem(value)).unwrap()
//     });
//     Ok(out.into_series())
// }

#[polars_expr(output_type=Float64)]
fn weighted_mean(inputs: &[Series]) -> PolarsResult<Series> {
    let values = inputs[0].list()?;
    let weights = &inputs[1].list()?;

    let out: Float64Chunked = binary_amortized_elementwise(
        values,
        weights,
        |values_inner: &Series, weights_inner: &Series| -> Option<f64> {
            let values_inner = values_inner.i64().unwrap();
            let weights_inner = weights_inner.f64().unwrap();
            let out_inner: Float64Chunked = binary_elementwise_values(
                values_inner,
                weights_inner,
                |value: i64, weight: f64| value as f64 * weight,
            );
            match (out_inner.sum(), weights_inner.sum()) {
                (Some(weighted_sum), Some(weights_sum)) => Some(weighted_sum / weights_sum),
                _ => None,
            }
        },
    );
    Ok(out.into_series())
}

fn shifted_struct(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    match field.data_type() {
        DataType::Struct(fields) => {
            let mut field_0 = fields[0].clone();
            let name = field_0.name().clone();
            field_0.set_name(fields[fields.len() - 1].name().clone());
            let mut fields = fields[1..]
                .iter()
                .zip(fields[0..fields.len() - 1].iter())
                .map(|(fld, name)| Field::new(name.name(), fld.data_type().clone()))
                .collect::<Vec<_>>();
            fields.push(field_0);
            Ok(Field::new(&name, DataType::Struct(fields)))
        }
        _ => unreachable!(),
    }
}

#[polars_expr(output_type_func=shifted_struct)]
fn shift_struct(inputs: &[Series]) -> PolarsResult<Series> {
    let struct_ = inputs[0].struct_()?;
    let fields = struct_.fields();
    if fields.is_empty() {
        return Ok(inputs[0].clone());
    }
    let mut field_0 = fields[0].clone();
    field_0.rename(fields[fields.len() - 1].name());
    let mut fields = fields[1..]
        .iter()
        .zip(fields[..fields.len() - 1].iter())
        .map(|(s, name)| {
            let mut s = s.clone();
            s.rename(name.name());
            s
        })
        .collect::<Vec<_>>();
    fields.push(field_0);
    StructChunked::new(struct_.name(), &fields).map(|ca| ca.into_series())
}

// use polars_arrow::array::MutablePlString;
// use polars_core::utils::align_chunks_binary;
// use reverse_geocoder::ReverseGeocoder;

// #[polars_expr(output_type=String)]
// fn reverse_geocode(inputs: &[Series]) -> PolarsResult<Series> {
//     let lhs = inputs[0].f64()?;
//     let rhs = inputs[1].f64()?;
//     let geocoder = ReverseGeocoder::new();

//     let (lhs, rhs) = align_chunks_binary(lhs, rhs);
//     let chunks = lhs
//         .downcast_iter()
//         .zip(rhs.downcast_iter())
//         .map(|(lhs_arr, rhs_arr)| {
//             let mut buf = String::new();
//             let mut mutarr = MutablePlString::with_capacity(lhs_arr.len());

//             for (lhs_opt_val, rhs_opt_val) in lhs_arr.iter().zip(rhs_arr.iter()) {
//                 match (lhs_opt_val, rhs_opt_val) {
//                     (Some(lhs_val), Some(rhs_val)) => {
//                         let res = &geocoder.search((*lhs_val, *rhs_val)).record.name;
//                         buf.clear();
//                         write!(buf, "{res}").unwrap();
//                         mutarr.push(Some(&buf))
//                     }
//                     _ => mutarr.push_null(),
//                 }
//             }

//             mutarr.freeze().boxed()
//         })
//         .collect();
//     let out: StringChunked = unsafe { ChunkedArray::from_chunks(lhs.name(), chunks) };
//     Ok(out.into_series())
// }

fn list_idx_dtype(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = Field::new(input_fields[0].name(), DataType::List(Box::new(IDX_DTYPE)));
    Ok(field.clone())
}

#[polars_expr(output_type_func=list_idx_dtype)]
fn non_zero_indices(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].list()?;

    let out: ListChunked = ca.apply_amortized(|s| {
        let s: &Series = s.as_ref();
        let ca: &Int64Chunked = s.i64().unwrap();
        let mut out: Vec<Option<u32>> = Vec::with_capacity(ca.len());
        for (idx, element) in ca.into_iter().enumerate() {
            match element {
                Some(0) => (),
                Some(_) => out.push(Some(idx as IdxSize)),
                None => (),
            }
        }
        let out: IdxCa = out.into_iter().collect_ca("");
        out.into_series()
    });
    Ok(out.into_series())
}

#[polars_expr(output_type_func=list_idx_dtype)]
fn distance_to_previous_larger_value(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let mut idx: Vec<Option<i32>> = Vec::with_capacity(ca.len());
    let out: Int32Chunked = ca
        .into_iter()
        .enumerate()
        .map(|(i, opt_val)| {
            let i_curr = i;
            if i == 0 {
                idx.push(None);
                return None
            }
            let mut i = Some((i as i32) - 1);
            while i.is_some() && opt_val.unwrap() >= ca.get(i.unwrap() as usize).unwrap() {
                i = idx[i.unwrap() as usize];
            }
            if i.is_none() {
                idx.push(None);
                return Some(0)
            }
            idx.push(i);
            Some(i_curr as i32 - i.unwrap())
        })
        .collect();
    Ok(out.into_series())
}
