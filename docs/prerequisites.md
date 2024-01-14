# 0. Prerequisites

## Knowledge

> "But you know what I like more than materialistic things? Knowledge." Tai Lopez

How much Rust do you need to know to write your own Polars plugin? Less than
you think.

If you pick up [The Rust Programming Language](https://doc.rust-lang.org/book/)
and can make it through the first 9 chapters, then I postulate
that you'll have enough knowledge at least 99% of inefficient `map_elements`
calls.
If you want to make a plugin which is generic enough that you can share
it with others, then you may need chapter 10 as well.

You'll also need basic Python knowledge: classes, decorators, and functions.

Alternatively, you could just clone this repo and then hack away
at the examples trial-and-error style until you get what you're looking
for - the compiler will probably help you more than you're expecting.

## Software

First, you should probably make new directory for this project.
Either clone https://github.com/MarcoGorelli/polars-plugins-tutorial,
or make a new directory.

Next, you'll need a Python3.8+ virtual environment with:

- `polars>=0.20.0`
- `maturin>=1.4.0` (but older versions _may_ work too)

installed.

You'll also need to have Rust installed, see [rustup](https://rustup.rs/) for
how to do that.

That's it! However, you are highly encouraged to also install
[rust-analyzer](https://rust-analyzer.github.io/manual.html) if you want to
improve your Rust-writing experience by exactly 120%.
