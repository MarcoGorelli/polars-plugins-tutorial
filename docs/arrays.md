
# 11. Array, captain!

We've talked about lists, structs, but what about arrays? As in, fixed sized arrays, e.g., x and y coordinates of 2d points:

```python
points = pl.Series(
    "points",
    [
        [6.63, 8.35],
        [7.19, 4.85],
        [2.1, 4.21],
        [3.4, 6.13],
        [2.48, 9.26],
        [9.41, 7.26],
        [7.45, 8.85],
        [6.58, 5.22],
        [6.05, 5.77],
        [8.57, 4.16],
        [3.22, 4.98],
        [6.62, 6.62],
        [9.36, 7.44],
        [8.34, 3.43],
        [4.47, 7.61],
        [4.34, 5.05],
        [5.0, 5.05],
        [5.0, 5.0],
        [2.07, 7.8],
        [9.45, 9.6],
        [3.1, 3.26],
        [4.37, 5.72],
    ],
    dtype=pl.Array(pl.Float64, 2),
)
df = pl.DataFrame(points)

print(df)
```

```
shape: (22, 1)
┌───────────────┐
│ points        │
│ ---           │
│ array[f64, 2] │
╞═══════════════╡
│ [6.63, 8.35]  │
│ [7.19, 4.85]  │
│ [2.1, 4.21]   │
│ [3.4, 6.13]   │
│ [2.48, 9.26]  │
│ …             │
│ [5.0, 5.0]    │
│ [2.07, 7.8]   │
│ [9.45, 9.6]   │
│ [3.1, 3.26]   │
│ [4.37, 5.72]  │
└───────────────┘
```

What if we wanted to make a plugin that takes a Series like `points` above, and likewise, returned a Series of 2d arrays?
Turns out we _can_ do it! But it's a little bit tricky.

Let's create a plugin that calculates the midpoint between a reference point and each point in a Series like the one above.
This should illustrate both how to unpack an array inside our rust code and also return a Series of the same type.

We'll start by registering our plugin:

```python
def midpoint_2d(expr: IntoExpr, ref_point: tuple[float, float]) -> pl.Expr:
    return register_plugin_function(
        args=[expr],
        plugin_path=Path(__file__).parent,
        function_name="midpoint_2d",
        is_elementwise=True,
        kwargs={"ref_point": ref_point},
    )
```

As you can see, we included an additional kwarg: `ref_point`, which we annotated with the type `tuple: [float, float]`.
In our rust code, we won't receive it as a tuple, though, it'll also be an array.
This isn't crucial for this example, so just accept it for now.
As you saw in the __arguments__ chapter, we take kwargs by defining a struct for them:

```rust
#[derive(Deserialize)]
struct MidPoint2DKwargs {
    ref_point: [f64; 2],
}
```

And we can finally move to the actual plugin code:

```rust
// We need this to ensure the output is of dtype array.
// Unfortunately, polars plugins do not support something similar to:
// #[polars_expr(output_type=Array)]
pub fn point_2d_output(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        "point_2d",
        DataType::Array(Box::new(DataType::Float64), 2),
    ))
}

#[polars_expr(output_type_func=point_2d_output)]
fn midpoint_2d(inputs: &[Series], kwargs: MidPoint2DKwargs) -> PolarsResult<Series> {
    let ca: &ArrayChunked = inputs[0].array()?;
    let ref_point = kwargs.ref_point;

    let out: Result<ArrayChunked, PolarsError> = unsafe {
        ca.try_apply_amortized_same_type(|row| {
            let s = row.as_ref();
            let ca = s.f64()?;
            let out_inner: Float64Chunked = ca
                .iter()
                .enumerate()
                .map(|(idx, opt_val)| {
                    opt_val.map(|val| {
                        (val + ref_point[idx]) / 2.0f64
                    })
                }).collect_trusted();
            Ok(out_inner.into_series())
        })};

    Ok(out?.into_series())
}
```

Uh-oh, unsafe, run to the hil- No, wait! It's true that we need unsafe here, but let's not freak out.
If we read the docs of `try_apply_amortized_same_type`, we see the following:

> ### Safety
> Return series of F must has the same dtype and number of elements as input if it is Ok.

In this example, we can uphold that contract - we know we're returning a Series with the same number of elements and same dtype as the input - take that, _unsafe_!

Still, the code looks a bit scary, doesn't it? So let's break it down:

```rust
let out: Result<ArrayChunked, PolarsError> = unsafe {

    // This is similar to apply_values, but it's amortized and made specifically
    // for scenarios in which we know both the return type and length will be
    // the same as the input. Since it's the try_* version of the function, it
    // also possibly handles the `?` we use in the closure later
    ca.try_apply_amortized_same_type(|row| {
        // `row` is officially an AmortSeries. What does that mean? Shouldn't it
        // be a 2d array with simple element access? Unfortunately not, but at
        // least we're on the right track: it does indeed contain two elements

        let s = row.as_ref();
        // We unpack it similarly to the way we've been unpacking Series in the
        // previous chapters:
        //
        // Previously we've been doing this to unpack a column we had behind a
        // Series - this time, inside this closure, the Series contains the two
        // elements composing the "row" (x and y):
        let ca = s.f64()?;

        // There are many ways to extract the x and y coordinates from ca.
        // Here, we remain idiomatic and consistent with what we've been doing
        // in the past - iterate, enumerate and map:
        let out_inner: Float64Chunked = ca
            .iter()
            .enumerate()
            .map(|(idx, opt_val)| {

                // We only use map here because opt_val is an Option
                opt_val.map(|val| {

                    // Here's where the simple logic of calculating a
                    // midpoint happens. We take the coordinate (`val`) at
                    // index `idx`, add it to the `idx-th` entry of our
                    // reference point (which is a coordinate of our point),
                    // then divide it by two, since we're dealing with 2d
                    // points only.
                    (val + ref_point[idx]) / 2.0f64
                })
                // Our map already returns Some or None, so we don't have to
                // worry about wrapping the result in, e.g., Some()
            }).collect_trusted();

        // At last, we convert out_inner (which is a Float64Chunked) back to a
        // Series
        Ok(out_inner.into_series())
    })};

// And finally, we convert our ArrayChunked into a Series, ready to ship to
// Python-land:
Ok(out?.into_series())
```

What does the result look like?
In `run.py`, we have:

```python
import polars as pl
# Change mp for whatever you chose to name your plugin, e.g., minimal_plugin
from mp import midpoint_2d

points = pl.Series(
    "points",
    [
        [6.63, 8.35],
        [7.19, 4.85],
        [2.1, 4.21],
        [3.4, 6.13],
        [2.48, 9.26],
        [9.41, 7.26],
        [7.45, 8.85],
        [6.58, 5.22],
        [6.05, 5.77],
        [8.57, 4.16],
        [3.22, 4.98],
        [6.62, 6.62],
        [9.36, 7.44],
        [8.34, 3.43],
        [4.47, 7.61],
        [4.34, 5.05],
        [5.0, 5.05],
        [5.0, 5.0],
        [2.07, 7.8],
        [9.45, 9.6],
        [3.1, 3.26],
        [4.37, 5.72],
    ],
    dtype=pl.Array(pl.Float64, 2),
)
df = pl.DataFrame(points)

# Now we call our plugin:
result = df.with_columns(midpoints=midpoint_2d("points", ref_point=(5.0, 5.0)))
print(result)
```

Let's compile and run it:
```shell
maturin develop

python run.py
```

🥁:
```
shape: (22, 2)
┌───────────────┬────────────────┐
│ points        ┆ midpoints      │
│ ---           ┆ ---            │
│ array[f64, 2] ┆ array[f64, 2]  │
╞═══════════════╪════════════════╡
│ [6.63, 8.35]  ┆ [5.815, 6.675] │
│ [7.19, 4.85]  ┆ [6.095, 4.925] │
│ [2.1, 4.21]   ┆ [3.55, 4.605]  │
│ [3.4, 6.13]   ┆ [4.2, 5.565]   │
│ [2.48, 9.26]  ┆ [3.74, 7.13]   │
│ …             ┆ …              │
│ [5.0, 5.0]    ┆ [5.0, 5.0]     │
│ [2.07, 7.8]   ┆ [3.535, 6.4]   │
│ [9.45, 9.6]   ┆ [7.225, 7.3]   │
│ [3.1, 3.26]   ┆ [4.05, 4.13]   │
│ [4.37, 5.72]  ┆ [4.685, 5.36]  │
└───────────────┴────────────────┘
```

Hurray, we did it!
