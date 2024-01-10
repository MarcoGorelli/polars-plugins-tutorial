use polars::prelude::arity::binary_elementwise;
use polars::prelude::*;

pub(crate) fn impl_add(left: &Int64Chunked, right: &Int64Chunked) -> Int64Chunked {
    binary_elementwise(left, right, |left, right| match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        _ => None,
    })
}
