use polars::prelude::*;
use polars_arrow::array::MutablePlString;
use polars_core::utils::align_chunks_binary;
use std::fmt::Write;
use polars_arrow::array::Array;

// This function is useful for writing functions which
// accept pairs of List columns. Delete if unneded.
#[allow(dead_code)]
pub(crate) fn binary_amortized_elementwise<'a, T, K, F>(
    ca: &'a ListChunked,
    weights: &'a ListChunked,
    mut f: F,
) -> ChunkedArray<T>
where
    T: PolarsDataType,
    T::Array: ArrayFromIter<Option<K>>,
    F: FnMut(&Series, &Series) -> Option<K> + Copy,
{
    unsafe {
        ca.amortized_iter()
            .zip(weights.amortized_iter())
            .map(|(lhs, rhs)| match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => f(lhs.as_ref(), rhs.as_ref()),
                _ => None,
            })
            .collect_ca(ca.name())
    }
}

use polars_core::prelude::arity::BinaryFnMut;

// This function is useful for writing functions which
// accept pairs of columns and produce String output. Delete if unneded.
#[allow(dead_code)]
pub(crate) fn binary_apply_to_buffer_generic<T, K, F>(
    lhs: &ChunkedArray<T>,
    rhs: &ChunkedArray<K>,
    mut f: F,
) -> StringChunked
where
    T: PolarsDataType,
    K: PolarsDataType,
    // T::Array: ArrayFromIter<Option<K>>,
    F: for<'a> FnMut(T::Physical<'a>, K::Physical<'a>) -> String,
{
    let (lhs, rhs) = align_chunks_binary(lhs, rhs);
    let chunks = lhs
        .downcast_iter()
        .zip(rhs.downcast_iter())
        .map(|(lhs_arr, rhs_arr)| {
            let mut buf = String::new();
            let mut mutarr = MutablePlString::with_capacity(lhs_arr.len());

            for (lhs_opt_val, rhs_opt_val) in lhs_arr.iter().zip(rhs_arr.iter()) {
                match (lhs_opt_val, rhs_opt_val) {
                    (Some(lhs_val), Some(rhs_val)) => {
                        let res = f(lhs_val, rhs_val);
                        buf.clear();
                        write!(buf, "{res}").unwrap();
                        mutarr.push(Some(&buf))
                    }
                    _ => mutarr.push_null(),
                }
            }

            mutarr.freeze().boxed()
        })
        .collect();
    unsafe { ChunkedArray::from_chunks("placeholder", chunks) }
}

