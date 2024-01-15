# Not-so-elementwise, my dear Watson

The operations we've seen so far have all been elementwise, e.g.:

- for each row, we calculated the absolute value
- for each row, we summed the respective values in two columns

Let's do something (completely) different - instead of working with
each row in isolation, we'll calculate something which depends on the
rows which precede it.

Let's try implementing `cum_sum`.

## Python side

Before you copy-and-paste `def add_numeric` and just change the function
name - wait a sec. Yes, you can do that, but there's one more detail
you need to take care of: `elementwise`. We'll set it to `False` here.
We'll see at the end of this chapter what happens if you incorrectly set it
to `True`.

Add this to `minimal_plugin/__init__.py`:
```python
def cum_sum(self) -> pl.Expr:
    return self._expr.register_plugin(
        lib=lib,
        symbol="cum_sum",
        is_elementwise=False,
    )
```

## Rust

Time to learn a new Rust function: `scan`!
If you're not familiar with it, please take a little break from this tutorial
and read [the scan docs](https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.scan).

Welcome back! Let's use our newfound scan-superpowers to implement `cum_sum`:
```Rust
#[polars_expr(output_type_func=same_output_type)]
fn cum_sum(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.i64()?;
    let out: Int64Chunked = ca
        .into_iter()
        .scan(None, |state: &mut Option<i64>, current: Option<i64>| {
            let sum = match (*state, current) {
                (Some(state_inner), Some(current)) => {
                    *state = Some(state_inner + current);
                    *state
                }
                (None, Some(current)) => {
                    *state = Some(current);
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
```
The general idea is:

- we hold the running cum in `state`
- we iterate over rows, initialising `state` to be `None`
- if the current row is `Some` and `state` is `None`,
  then set `state` to the current row's value
- if the current row is `Some` and `state` is `Some`, then
  add the current row's value to `state`
- if the current row is `None`, then don't modify `state`
  and emit `None`.

Note how we use `collect_trusted` at the end, rather than `collect`.
`collect` would work as well, but if we know the length of the output,
then `collect_trusted` is faster. In the case of `cum_sum`, the length
of the input doesn't change, so we can safely use `collect_trusted` and
save some precious time.

## Did we really need `elementwise=False`?

If you try running
```python
df = pl.DataFrame({
    'a': [1, 2, 3, 4, None, 5],
    'b': [1, 1, 1, 2, 2, 2],
})
print(df.with_columns(a_cum_sum=pl.col('a').mp.cum_sum()))
```
you may notice that the result is identical, regardless of whether we set `elementwise`
to `True` or `False` - they both yield:
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

But wait - what happens if we throw a window function in there?
```python
print(df.with_columns(a_cum_sum=pl.col('a').mp.cum_sum().over('b')))
```

Now, we get:

- `elementwise=True`:

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

- `elementwise=False`:

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
