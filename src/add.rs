
use polars::prelude::*;
use polars::prelude::arity::binary_elementwise;

pub(crate) fn impl_add(left: &Int64Chunked, right: &Int64Chunked) -> Int64Chunked {
    let out: Int64Chunked = match right.len() {
        1 => {
            let right = right.get(0).unwrap();
            left.apply(|left|
                left.map(|left| left + right)
            )
        }
        _ => {
            binary_elementwise(
                left, right,
                |left, right|
                match (left, right) {
                    (Some(left), Some(right)) => Some(left + right),
                    _ => None
                }
            )
        }
    };
    out
}
