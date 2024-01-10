use polars::prelude::*;

pub(crate) fn impl_cum_sum(s: &Int64Chunked) -> Int64Chunked {
    // ooh...you need to modify state!
    s.into_iter()
        .scan(None, |state: &mut Option<i64>, x| {
            println!("state: {:?}, x: {:?}", state, x);
            let sum = match (*state, x) {
                (Some(_state), Some(x)) => {
                    *state = Some(_state + x);
                    Some(_state + x)
                }
                (Some(_state), None) => Some(_state),
                (None, Some(x)) => {
                    *state = Some(x);
                    Some(x)
                }
                (None, None) => None,
            };
            Some(sum)
        })
        .collect()
}
