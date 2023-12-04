
use polars::prelude::*;
use pyo3_polars::derive::{polars_expr};
use polars::prelude::arity::binary_elementwise;

#[polars_expr(output_type=Int64)]
fn add(inputs: &[Series]) -> PolarsResult<Series> {
    let left = inputs[0].i64()?;
    let right = inputs[1].i64()?;
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
    Ok(out.into_series())
}
