# How you (yes, you!) can write a Polars Plugin

- ✅ Unlock super-high performance
- ✅ Have a tonne of fun
- ✅ Impress everybody with your superpowers

![](assets/image.png){: style="width:400px"}

## Why?

Polars is an incredible and groundbreaking Dataframe library, and its expressions API
is simply amazing. Sometimes, however, you need to express really custom business logic
which just isn't in scope for the Polars API. In that situation, people tend to use
`map_elements`, which lets you express anything but also kills most of Polars' benefits.

But it doesn't have to be that way - with just basic Rust knowledge and this tutorial,
I postulate that you'll be able to address at least 99% of inefficient `map_elements` tasks!

## What will you learn

- Writing simple single-column elementwise expressions
- Writing complex multi-column non-elementwise expressions which use third-party Rust packages
- How to share your plugin superpowers with others

## What are people saying?

**Nelson Griffiths**, Engineering & ML Lead at Double River Investments | Core Maintainer Functime

> this was an awesome intro. I am no rust expert, though I have written a few plugins. And I learned quite a bit from this! Having my team read it now as well. Thanks for putting this together. I think more content like this for people who don’t know how to write optimal polars code on the rust side will be really useful for people like me who want to work on plugins!

**Barak David**, Software Engineer

> Amazing tutorial! I just created nltk plugin, and experienced X50 speedup!
