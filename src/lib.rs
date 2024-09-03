mod expressions;
mod arrays;

use pyo3_polars::PolarsAllocator;

#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();
