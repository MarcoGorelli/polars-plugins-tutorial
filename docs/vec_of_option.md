
# 12. `Vec<Option<T>>` vs. `Vec<T>`

> "I got, I got, I got, I got options" – _Pitbull_, before writing his first Polars plugin

In the plugins we're looked at so far, we typically created an iterator of `Option`s and let Polars collect it into a `ChunkedArray`.
Sometimes, however, you need to store intermediate values in a `Vec`. You might be tempted to make it a `Vec<Option<T>>`, where
missing values are `None` and present values are `Some`...

🛑 BUT WAIT!

Did you know that `Vec<Option<i32>>` occupies twice as much memory as `Vec<i32>`? Let's prove it:

```rust
use std::mem::size_of_val;

fn main() {
    let vector: Vec<i32> = vec![1, 2, 3];
    println!("{}", size_of_val(&*vector));
    // Output: 12

    let vector: Vec<Option<i32>> = vec![Some(1), Some(2), Some(3)];
    println!("{}", size_of_val(&*vector));
    // Output: 24
}
```

So...how can we create an output which includes missing values, without allocating twice as much memory as is necessary?

## Validity mask

Instead of creating a vector of `Option`s, we can create a vector of primitive values with zeroes in place of the missing values, and use
a validity mask to indicate which values are missing. One example of this can be seen in Polars' `interpolate_impl`, which does the heavy lifting for the
[`Series.interpolate`](https://docs.pola.rs/api/python/version/0.18/reference/series/api/polars.Series.interpolate.html):

```rust
fn interpolate_impl<T, I>(chunked_arr: &ChunkedArray<T>, interpolation_branch: I) -> ChunkedArray<T>
where
    T: PolarsNumericType,
    I: Fn(T::Native, T::Native, IdxSize, T::Native, &mut Vec<T::Native>),
{
    // This implementation differs from pandas as that boundary None's are not removed.
    // This prevents a lot of errors due to expressions leading to different lengths.
    if !chunked_arr.has_nulls() || chunked_arr.null_count() == chunked_arr.len() {
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
            T::get_dtype().to_arrow(CompatLevel::newest()),
            out.into(),
            Some(validity.into()),
        );
        ChunkedArray::with_chunk(chunked_arr.name(), array)
    } else {
        ChunkedArray::from_vec(chunked_arr.name(), out)
    }
}
```

That's a lot to digest at once, so let's take small steps and focus on the core logic.
At the start, we store the indexes of the first and last non-null values:

```rust
let first = chunked_arr.first_non_null().unwrap();
let last = chunked_arr.last_non_null().unwrap() + 1;
```

We then create a vector `out` to store the result values in, and in places where we'd like
the output to be missing, we push zeroes (we'll see below how we tell Polars that these are
to be considered missing, rather than as ordinary zeroes):

```rust
let mut out = Vec::with_capacity(chunked_arr.len());
for _ in 0..first {
    out.push(Zero::zero());
}
```

We then skip the first `first` elements and start interpolating (note how we write `out.push(low)`, not `out.push(Some(low))`
- we gloss over the rest as it's not related to the main focus of this chapter):

```rust
let mut iter = chunked_arr.iter().skip(first);
let mut low = iter.next().unwrap().unwrap();
out.push(low);
while let Some(next) = iter.next() {
    // Interpolation logic
}
```

Now, after _most_ of the work is done and we've filled up the `out` `Vec` (except for trailing missing values),
we create a validiy mask and set it to `false` for elements which we'd like to show up as missing:

```rust
if first != 0 || last != chunked_arr.len() {
    // A validity mask is created for the vector, initially all set to true
    let mut validity = MutableBitmap::with_capacity(chunked_arr.len());
    validity.extend_constant(chunked_arr.len(), true);

    for i in 0..first {
        // The indexes corresponding to the zeroes before the first valid value
        // are set to false (invalid)
        validity.set(i, false);
    }

    for i in last..chunked_arr.len() {
        // The indexes corresponding to the values after the last valid value
        // are set to false (invalid)
        validity.set(i, false);

        out.push(Zero::zero())  // Push zeroes after the last valid value, as
                                // many as there are nulls at the end, just like
                                // it was done before the first valid value.
    }

    let array = PrimitiveArray::new(
        T::get_dtype().to_arrow(CompatLevel::newest()),
        out.into(),
        Some(validity.into()),
    );
    ChunkedArray::with_chunk(chunked_arr.name(), array)
} else {
    ChunkedArray::from_vec(chunked_arr.name(), out)
}
```

Notice how only the outer nulls were set to invalid (the inner ones were filled in by the interpolation function).
The validity mask is only allocated when there are no outer nulls (`if first != 0 || last != chunked_arr.len()`).
Each element of a `MutableBitmap` only takes up a single bit, so the total space used is much less than it would've been
if we'd created `out` as a vector of `Option`s!

## Sentinel values

In some other cases, you may be able to avoid allocating a `Vec<Option<T>>` by using values known to be invalid for the use
case, but supported by the type `T`. For instance, if you have a `Vec<i32>` that represents indices, negative values
shouldn't ever be present in that vector.
By leveraging this information, you could actually `-1` to represent invalid elements.

Take a look at this diff from a PR from the Polars plugin `polars-xdt` that does exactly that:
[link](https://github.com/pola-rs/polars-xdt/pull/79/files#diff-991878a926639bba03bcc36a2790f73181b358f2ff59e0256f9ad76aa707be35)

The gist of the PR is changes such as:

```diff
-            if i < Some(0) {
-                idx.push(None);
+            if i < 0 {
+                idx.push(-1);
```

The check is done either way, so there's no runtime penalty for choosing one over the other.
But memory-wise we already know the benefits!

## Conclusion

In general, _if you can avoid allocating `Vec<Option<T>>` instead of `Vec<T>`,_ __do it!__
Of course this doesn't apply in every situation, but it's something important for plugin developers to have in mind.
