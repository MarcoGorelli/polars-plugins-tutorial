#![allow(clippy::unused_unit)]
use std::ops::{Add, Div, Mul, Sub};

use num_traits::{NumCast, Zero, Signed};
use polars::prelude::arity::{
    binary_elementwise_into_string_amortized, broadcast_binary_elementwise,
};
use polars::prelude::*;
use polars_arrow::bitmap::MutableBitmap;
use polars_core::series::amortized_iter::AmortSeries;
use polars_core::utils::align_chunks_binary;
use pyo3_polars::derive::polars_expr;
use pyo3_polars::export::polars_core::utils::arrow::array::PrimitiveArray;
use pyo3_polars::export::polars_core::utils::CustomIterTools;
use serde::Deserialize;

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
        },
    }
}

#[polars_expr(output_type=Int64)]
fn sum_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let left: &Int64Chunked = inputs[0].i64()?;
    let right: &Int64Chunked = inputs[1].i64()?;
    // Note: there's a faster way of summing two columns, see
    // section 7.
    let out: Int64Chunked =
        broadcast_binary_elementwise(left, right, |left: Option<i64>, right: Option<i64>| match (
            left, right,
        ) {
            (Some(left), Some(right)) => Some(left + right),
            _ => None,
        });
    Ok(out.into_series())
}

#[polars_expr(output_type_func=same_output_type)]
fn cum_sum(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca: &Int64Chunked = s.i64()?;
    let out: Int64Chunked = ca
        .iter()
        .scan(0_i64, |state: &mut i64, x: Option<i64>| match x {
            Some(x) => {
                *state += x;
                Some(Some(*state))
            },
            None => Some(None),
        })
        .collect_trusted();
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
                Cow::Borrowed(value)
            }
        })
    });
    Ok(out.into_series())
}

fn remove_last_extension(s: &str) -> &str {
    match s.rfind('.') {
        Some(pos) => &s[..pos],
        None => s,
    }
}

#[polars_expr(output_type=String)]
fn remove_extension(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;
    let out: StringChunked = ca.apply_values(|val| {
        let res = Cow::Borrowed(remove_last_extension(val));
        res
    });
    Ok(out.into_series())
}

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
    let out = Int64Chunked::from_chunk_iter(PlSmallStr::EMPTY, chunks);
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
    let out = ca.apply_into_string_amortized(|value, output| {
        write!(output, "{}{}", value, kwargs.suffix).unwrap();
    });
    Ok(out.into_series())
}

// use rust_stemmers::{Algorithm, Stemmer};

// #[polars_expr(output_type=String)]
// fn snowball_stem(inputs: &[Series]) -> PolarsResult<Series> {
//     let ca: &StringChunked = inputs[0].str()?;
//     let en_stemmer = Stemmer::create(Algorithm::English);
//     let out: StringChunked = ca.apply_into_string_amortized(|value: &str, output: &mut String| {
//         write!(output, "{}", en_stemmer.stem(value)).unwrap()
//     });
//     Ok(out.into_series())
// }

fn binary_amortized_elementwise<'a, T, K, F>(
    lhs: &'a ListChunked,
    rhs: &'a ListChunked,
    mut f: F,
) -> ChunkedArray<T>
where
    T: PolarsDataType,
    T::Array: ArrayFromIter<Option<K>>,
    F: FnMut(&AmortSeries, &AmortSeries) -> Option<K> + Copy,
{
    {
        let (lhs, rhs) = align_chunks_binary(lhs, rhs);
        lhs.amortized_iter()
            .zip(rhs.amortized_iter())
            .map(|(lhs, rhs)| match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => f(&lhs, &rhs),
                _ => None,
            })
            .collect_ca(PlSmallStr::EMPTY)
    }
}

#[polars_expr(output_type=Float64)]
fn weighted_mean(inputs: &[Series]) -> PolarsResult<Series> {
    let values = inputs[0].list()?;
    let weights = &inputs[1].list()?;
    polars_ensure!(
        values.dtype() == &DataType::List(Box::new(DataType::Int64)),
        ComputeError: "Expected `values` to be of type `List(Int64)`, got: {}", values.dtype()
    );
    polars_ensure!(
        weights.dtype() == &DataType::List(Box::new(DataType::Float64)),
        ComputeError: "Expected `weights` to be of type `List(Float64)`, got: {}", weights.dtype()
    );

    let out: Float64Chunked = binary_amortized_elementwise(
        values,
        weights,
        |values_inner: &AmortSeries, weights_inner: &AmortSeries| -> Option<f64> {
            let values_inner = values_inner.as_ref().i64().unwrap();
            let weights_inner = weights_inner.as_ref().f64().unwrap();
            if values_inner.is_empty() {
                // Mirror Polars, and return None for empty mean.
                return None;
            }
            let mut numerator: f64 = 0.;
            let mut denominator: f64 = 0.;
            values_inner
                .iter()
                .zip(weights_inner.iter())
                .for_each(|(v, w)| {
                    if let (Some(v), Some(w)) = (v, w) {
                        numerator += v as f64 * w;
                        denominator += w;
                    }
                });
            Some(numerator / denominator)
        },
    );
    Ok(out.into_series())
}

fn struct_point_2d_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    match field.dtype() {
        DataType::Struct(fields) => Ok(Field::new(
            "struct_point_2d".into(),
            DataType::Struct(fields.clone()),
        )),
        dtype => polars_bail!(InvalidOperation: "expected Struct dtype, got {}", dtype),
    }
}

#[polars_expr(output_type_func=struct_point_2d_output)]
fn print_struct_fields(inputs: &[Series]) -> PolarsResult<Series> {
    let struct_ = inputs[0].struct_()?;
    let fields = struct_.fields_as_series();

    if fields.is_empty() {
        return Ok(inputs[0].clone());
    }

    let fields = fields
        .iter()
        .map(|s| {
            let s = s.clone();
            println!("{:?}", s);
            s
        })
        .collect::<Vec<_>>();

    StructChunked::from_series(struct_.name().clone(), struct_.len(), fields.iter())
        .map(|ca| ca.into_series())
}

fn shifted_struct(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    match field.dtype() {
        DataType::Struct(fields) => {
            let mut field_0 = fields[0].clone();
            let name = field_0.name.clone();
            field_0.set_name(fields[fields.len() - 1].name().clone());
            let mut fields = fields[1..]
                .iter()
                .zip(fields[0..fields.len() - 1].iter())
                .map(|(fld, name)| Field::new(name.name().clone(), fld.dtype().clone()))
                .collect::<Vec<_>>();
            fields.push(field_0);
            Ok(Field::new(name, DataType::Struct(fields)))
        },
        _ => unreachable!(),
    }
}

#[polars_expr(output_type_func=shifted_struct)]
fn shift_struct(inputs: &[Series]) -> PolarsResult<Series> {
    let struct_ = inputs[0].struct_()?;
    let fields = struct_.fields_as_series();
    if fields.is_empty() {
        return Ok(inputs[0].clone());
    }
    let mut field_0 = fields[0].clone();
    let name = field_0.name().clone();
    field_0.rename(fields[fields.len() - 1].name().clone());
    let mut fields = fields[1..]
        .iter()
        .zip(fields[..fields.len() - 1].iter())
        .map(|(s, name)| {
            let mut s = s.clone();
            s.rename(name.name().clone());
            s
        })
        .collect::<Vec<_>>();
    fields.push(field_0);
    StructChunked::from_series(name, struct_.len(), fields.iter()).map(|ca| ca.into_series())
}

use reverse_geocoder::ReverseGeocoder;

#[polars_expr(output_type=String)]
fn reverse_geocode(inputs: &[Series]) -> PolarsResult<Series> {
    let latitude = inputs[0].f64()?;
    let longitude = inputs[1].f64()?;
    let geocoder = ReverseGeocoder::new();
    let out = binary_elementwise_into_string_amortized(latitude, longitude, |lhs, rhs, out| {
        let search_result = geocoder.search((lhs, rhs));
        write!(out, "{}", search_result.record.name).unwrap();
    });
    Ok(out.into_series())
}

fn list_idx_dtype(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = Field::new(
        input_fields[0].name.clone(),
        DataType::List(Box::new(IDX_DTYPE)),
    );
    Ok(field.clone())
}

#[polars_expr(output_type_func=list_idx_dtype)]
fn non_zero_indices(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].list()?;
    polars_ensure!(
        ca.dtype() == &DataType::List(Box::new(DataType::Int64)),
        ComputeError: "Expected `List(Int64)`, got: {}", ca.dtype()
    );

    let out: ListChunked = ca.apply_amortized(|s| {
        let s: &Series = s.as_ref();
        let ca: &Int64Chunked = s.i64().unwrap();
        let out: IdxCa = ca
            .iter()
            .enumerate()
            .filter(|(_idx, opt_val)| opt_val != &Some(0))
            .map(|(idx, _opt_val)| Some(idx as IdxSize))
            .collect_ca(PlSmallStr::EMPTY);
        out.into_series()
    });
    Ok(out.into_series())
}

#[polars_expr(output_type=Float64)]
fn vertical_weighted_mean(inputs: &[Series]) -> PolarsResult<Series> {
    let values = &inputs[0].f64()?;
    let weights = &inputs[1].f64()?;
    let mut numerator = 0.;
    let mut denominator = 0.;
    values.iter().zip(weights.iter()).for_each(|(v, w)| {
        if let (Some(v), Some(w)) = (v, w) {
            numerator += v * w;
            denominator += w;
        }
    });
    let result = numerator / denominator;
    Ok(Series::new(PlSmallStr::EMPTY, vec![result]))
}

fn linear_itp<T>(low: T, step: T, slope: T) -> T
where
    T: Sub<Output = T> + Mul<Output = T> + Add<Output = T> + Div<Output = T>,
{
    low + step * slope
}

#[inline]
fn signed_interp<T>(low: T, high: T, steps: IdxSize, steps_n: T, out: &mut Vec<T>)
where
    T: Sub<Output = T> + Mul<Output = T> + Add<Output = T> + Div<Output = T> + NumCast + Copy,
{
    let slope = (high - low) / steps_n;
    for step_i in 1..steps {
        let step_i: T = NumCast::from(step_i).unwrap();
        let v = linear_itp(low, step_i, slope);
        out.push(v)
    }
}

fn interpolate_impl<T, I>(chunked_arr: &ChunkedArray<T>, interpolation_branch: I) -> ChunkedArray<T>
where
    T: PolarsNumericType,
    I: Fn(T::Native, T::Native, IdxSize, T::Native, &mut Vec<T::Native>),
{
    // This implementation differs from pandas as that boundary None's are not removed.
    // This prevents a lot of errors due to expressions leading to different lengths.
    if chunked_arr.null_count() == 0 || chunked_arr.null_count() == chunked_arr.len() {
        return chunked_arr.clone();
    }

    // We first find the first and last so that we can set the null buffer.
    let first = chunked_arr.first_non_null().unwrap();
    let last = chunked_arr.last_non_null().unwrap() + 1;

    // Fill out with `first` nulls.
    let mut out = Vec::with_capacity(chunked_arr.len());
    let mut iter = chunked_arr.iter().skip(first);
    for _ in 0..first {
        out.push(Zero::zero());
    }

    // The next element of `iter` is definitely `Some(Some(v))`, because we skipped the first
    // elements `first` and if all values were missing we'd have done an early return.
    let mut low = iter.next().unwrap().unwrap();
    out.push(low);
    while let Some(next) = iter.next() {
        if let Some(v) = next {
            out.push(v);
            low = v;
        } else {
            let mut steps = 1 as IdxSize;
            for next in iter.by_ref() {
                steps += 1;
                if let Some(high) = next {
                    let steps_n: T::Native = NumCast::from(steps).unwrap();
                    interpolation_branch(low, high, steps, steps_n, &mut out);
                    out.push(high);
                    low = high;
                    break;
                }
            }
        }
    }
    if first != 0 || last != chunked_arr.len() {
        let mut validity = MutableBitmap::with_capacity(chunked_arr.len());
        validity.extend_constant(chunked_arr.len(), true);

        for i in 0..first {
            validity.set(i, false);
        }

        for i in last..chunked_arr.len() {
            validity.set(i, false);
            out.push(Zero::zero())
        }

        let array = PrimitiveArray::new(
            T::get_static_dtype().to_arrow(CompatLevel::newest()),
            out.into(),
            Some(validity.into()),
        );
        ChunkedArray::with_chunk(PlSmallStr::EMPTY, array)
    } else {
        ChunkedArray::from_vec(PlSmallStr::EMPTY, out)
    }
}

#[polars_expr(output_type=Int64)]
fn interpolate(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.i64()?;
    let out: Int64Chunked = interpolate_impl(ca, signed_interp::<i64>);
    Ok(out.into_series())
}

#[polars_expr(output_type=Int64)]
fn life_step(inputs: &[Series]) -> PolarsResult<Series> {
    let (ca_lf, ca_curr, ca_rt) = (inputs[0].i64()?, inputs[1].i64()?, inputs[2].i64()?);

    let lf = ca_lf
        .cont_slice()
        .expect("Expected input to be contiguous (in a single chunk)");
    let mid = ca_curr
        .cont_slice()
        .expect("Expected input to be contiguous (in a single chunk)");
    let rt = ca_rt
        .cont_slice()
        .expect("Expected input to be contiguous (in a single chunk)");

    let len = lf.len();

    let out: Int64Chunked = mid
        .iter()
        .enumerate()
        .map(|(idx, val)| {
            // Neighbours above
            let prev_row = if 0 == idx {
                lf[len - 1] + mid[len - 1] + rt[len - 1]
            } else {
                lf[idx - 1] + mid[idx - 1] + rt[idx - 1]
            };

            // Curr row does not include cell in the middle,
            // a cell is not a neighbour of itself
            let curr_row = lf[idx] + rt[idx];

            // Neighbours below
            let next_row = if len - 1 == idx {
                lf[0] + mid[0] + rt[0]
            } else {
                lf[idx + 1] + mid[idx + 1] + rt[idx + 1]
            };

            // Life logic
            Some(match (val, prev_row + curr_row + next_row) {
                (1, 2) | (1, 3) => 1,
                (0, 3) => 1,
                _ => 0,
            })
        })
        .collect_trusted();
    Ok(out.into_series())
}
