#![allow(clippy::unused_unit)]
use polars::prelude::*;
use polars_core::utils::CustomIterTools;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;

pub fn point_2d_output(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        PlSmallStr::from_static("point_2d"),
        DataType::Array(Box::new(DataType::Float64), 2),
    ))
}

#[derive(Deserialize)]
struct MidPoint2DKwargs {
    ref_point: [f64; 2],
}

#[polars_expr(output_type_func=point_2d_output)]
fn midpoint_2d(inputs: &[Series], kwargs: MidPoint2DKwargs) -> PolarsResult<Series> {
    let ca: &ArrayChunked = inputs[0].array()?;
    let ref_point = kwargs.ref_point;

    let out: ArrayChunked = unsafe {
        ca.try_apply_amortized_same_type(|row| {
            let s = row.as_ref();
            let ca = s.f64()?;
            let out_inner: Float64Chunked = ca
                .iter()
                .enumerate()
                .map(|(idx, opt_val)| opt_val.map(|val| (val + ref_point[idx]) / 2.0f64))
                .collect_trusted();
            Ok(out_inner.into_series())
        })
    }?;

    Ok(out.into_series())
}
