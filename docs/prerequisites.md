# 0. Prerequisites

## Knowledge

> "But you know what I like more than materialistic things? Knowledge." Tai Lopez

How much Rust do you need to know to write your own Polars plugin? Less than
you think.

If you pick up [The Rust Programming Language](https://doc.rust-lang.org/book/)
and can make it through the first 10 chapters, then I postulate
that you'll have enough knowledge to replace the vast majority of
inefficient `map_elements` calls.

You'll also need basic Python knowledge: classes, decorators, and functions.

Alternatively, you could just clone this repo and then hack away
at the examples trial-and-error style until you get what you're looking
for - the compiler will probably help you more than you're expecting.

## Software

The fastest way to get started is to use
[cookiecutter-polars-plugins](https://github.com/MarcoGorelli/cookiecutter-polars-plugins).
Otherwise, make a new directory for this project
or clone https://github.com/MarcoGorelli/polars-plugins-tutorial,

Next, you'll need a Python3.8+ virtual environment with:

- `polars>=0.20.0`
- `maturin>=1.4.0` (but older versions _may_ work too)

installed.

You'll also need to have Rust installed, see [rustup](https://rustup.rs/) for
how to do that.

That's it! However, you are highly encouraged to also install
[rust-analyzer](https://rust-analyzer.github.io/manual.html) if you want to
improve your Rust-writing experience by exactly 120%.

## What's in a Series?

If you take a look at a Series such as
```python
In [9]: s = pl.Series([None, 2, 3]) + 42

In [10]: s
Out[10]:
shape: (3,)
Series: '' [i64]
[
        null
        44
        45
]
```
you may be tempted to conclude that it contains three values: `[null, 1, 2]`.

However, if you print out `s._get_buffer(0)` and `s._get_buffer(1)`, you'll see
something different:

- `s._get_buffer(0)`: `[42, 44, 45]`. These are the _values_.
- `s._get_buffer(1)`: `[False, True, True]`. These are the _validities_.

So we don't really have integers and `null` mixed together into a single array - we
have a pair of arrays, one holding values and another one holding booleans indicating
whether each value is valid or not.
If a value appears as `null` to you, then there's no guarantee about what physical number
is behind it! It was `42` here, but it could well be `43`, or any other number,
in another example.

## What's a chunk?

A Series is backed by chunked arrays, each of which holds data which is contiguous in
memory.

Here's an example of a Series backed  by multiple chunks:
```python
In [27]: s = pl.Series([1,2,3])

In [28]: s = s.append(pl.Series([99, 11]))

In [29]: s
Out[29]:
shape: (5,)
Series: '' [i64]
[
        1
        2
        3
        99
        11
]

In [30]: s.get_chunks()
Out[30]:
[shape: (3,)
 Series: '' [i64]
 [
        1
        2
        3
 ],
 shape: (2,)
 Series: '' [i64]
 [
        99
        11
 ]]
```
Chunked arrays will come up in several examples in this tutorial.
