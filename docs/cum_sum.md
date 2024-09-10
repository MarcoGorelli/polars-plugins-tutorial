# 4. Yes we SCAN

The operations we've seen so far have all been elementwise, e.g.:

- for each row, we calculated the absolute value
- for each row, we summed the respective values in two columns

Let's do something (completely) different - instead of working with
each row in isolation, we'll calculate a quantity which depends on the
rows which precede it.

We're going to implement `cum_sum`.

## Python side

Add this to `minimal_plugin/__init__.py`:
```python
def cum_sum(expr: IntoExpr) -> pl.Expr:
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="cum_sum",
        is_elementwise=False,
    )
```
Note how, unlike in previous examples, we set `is_elementwise=False`.
You'll see why this is so important at the end of this page.

## Rust

Time to learn a new Rust function: `scan`.
If you're not familiar with it, please take a little break from this tutorial
and [read the scan docs](https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.scan).

Welcome back! Let's use our newfound scan-superpowers to implement `cum_sum`. Here's what goes into `src/expressions.rs`:
```Rust
#[polars_expr(output_type_func=same_output_type)]
fn cum_sum(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca: &Int64Chunked = s.i64()?;
    let out: Int64Chunked = ca
        .iter()
        .scan(0_i64, |state: &mut i64, x: Option<i64>| {
            match x {
                Some(x) => {
                    *state += x;
                    Some(Some(*state))
                },
                None => Some(None),
            }
        })
        .collect_trusted();
    let out: Int64Chunked = out.with_name(ca.name());
    Ok(out.into_series())
}
```
Make sure to also add
```Rust
use pyo3_polars::export::polars_core::utils::CustomIterTools;
```
to the top of the file.

The `cum_sum` definition may look complex, but it's not too bad once we
break it down:

- we hold the running sum in `state`
- we iterate over rows, initialising `state` to be `0`
- if the current row is `Some`, then add the current row's value to `state` and emit the current value of `state`
- if the current row is `None`, then don't modify `state` and emit `None`

Note how we use `collect_trusted` at the end, rather than `collect`.
`collect` would work as well, but if we know the length of the output
(and we do in this case, `cum_sum` doesn't change the column's length)
then we can safely use `collect_trusted` and save some precious time.

Let's compile with `maturin develop` (or `maturin develop --release`
if you're benchmarking), change the last line of `run.py` to
```python
print(df.with_columns(a_cum_sum=mp.cum_sum('a')))
```
and then run `python run.py`:

```
shape: (3, 3)
┌─────┬──────┬───────────┐
│ a   ┆ b    ┆ a_cum_sum │
│ --- ┆ ---  ┆ ---       │
│ i64 ┆ i64  ┆ i64       │
╞═════╪══════╪═══════════╡
│ 1   ┆ 3    ┆ 1         │
│ 5   ┆ null ┆ 6         │
│ 2   ┆ -1   ┆ 8         │
└─────┴──────┴───────────┘
```

## Elementwise, my dear Watson

Why was it so important to set `is_elementwise` correctly? Let's see
with an example.

Put the following in `run.py`:
```python
import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({
    'a': [1, 2, 3, 4, None, 5],
    'b': [1, 1, 1, 2, 2, 2],
})
print(df.with_columns(a_cum_sum=mp.cum_sum('a')))
```

Then, run `python run.py`.

Finally, go to `minimal_plugin/__init__.py` and change `is_elementwise`
from `False` to `True`, and run `python run.py` again.

In both cases, you should see the following output:
```
shape: (6, 3)
┌──────┬─────┬───────────┐
│ a    ┆ b   ┆ a_cum_sum │
│ ---  ┆ --- ┆ ---       │
│ i64  ┆ i64 ┆ i64       │
╞══════╪═════╪═══════════╡
│ 1    ┆ 1   ┆ 1         │
│ 2    ┆ 1   ┆ 3         │
│ 3    ┆ 1   ┆ 6         │
│ 4    ┆ 2   ┆ 10        │
│ null ┆ 2   ┆ null      │
│ 5    ┆ 2   ┆ 15        │
└──────┴─────┴───────────┘
```
which looks correct. So, what's the deal with `is_elementwise`?

The deal is that we need it in order for window functions / `group_by`s
to be correct. Change the last line of `run.py` to
```python
print(df.with_columns(a_cum_sum=mp.cum_sum('a').over('b')))
```

Now, we get:

- with `elementwise=True`:

    ```
    shape: (6, 3)
    ┌──────┬─────┬───────────┐
    │ a    ┆ b   ┆ a_cum_sum │
    │ ---  ┆ --- ┆ ---       │
    │ i64  ┆ i64 ┆ i64       │
    ╞══════╪═════╪═══════════╡
    │ 1    ┆ 1   ┆ 1         │
    │ 2    ┆ 1   ┆ 3         │
    │ 3    ┆ 1   ┆ 6         │
    │ 4    ┆ 2   ┆ 10        │
    │ null ┆ 2   ┆ null      │
    │ 5    ┆ 2   ┆ 15        │
    └──────┴─────┴───────────┘
    ```

- with `elementwise=False`:

    ```
    shape: (6, 3)
    ┌──────┬─────┬───────────┐
    │ a    ┆ b   ┆ a_cum_sum │
    │ ---  ┆ --- ┆ ---       │
    │ i64  ┆ i64 ┆ i64       │
    ╞══════╪═════╪═══════════╡
    │ 1    ┆ 1   ┆ 1         │
    │ 2    ┆ 1   ┆ 3         │
    │ 3    ┆ 1   ┆ 6         │
    │ 4    ┆ 2   ┆ 4         │
    │ null ┆ 2   ┆ null      │
    │ 5    ┆ 2   ┆ 9         │
    └──────┴─────┴───────────┘
    ```

Only `elementwise=False` actually respected the window! This is why
it's important to set `elementwise` correctly.
