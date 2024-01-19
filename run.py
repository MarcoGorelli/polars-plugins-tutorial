import polars as pl
import minimal_plugin  # noqa: F401

# df = pl.DataFrame({'word': ['I', 'can', 'speak', 'pig', 'latin', 'and', 'so']})
# vowels = {'a', 'e', 'i', 'o', 'u'}
# def pig_latinnify_python(s: str) -> str:
#     if s:
#         if s[0].lower() in vowels:
#             return s + 'yay'
#         return s[1:] + s[0] + 'ay'
#     return s
# print(df.with_columns(word_pig_latin=pl.col('word').map_elements(pig_latinnify_python)))
# print(df.select(pl.col('word').mp.pig_latinnify_2()))

# Weighted mean: not easy, but not too bad
weights = [1,1,1.5,2,2,1.5,1,2,3,2]
df = pl.DataFrame({
    'values': [[14., 19, 22,25,29,31,31,38,40,41]],
})
print(df)
print(df.with_columns(
    w_mean = pl.col('values').list.eval(pl.element()*pl.Series(weights)).list.sum()/ pl.Series(weights).sum()
))

print(df.with_columns(
    w_mean = pl.col('values').mp.weighted_mean(pl.Series(weights))
))
# Weighted standard deviation: friggin' impossible!

df = pl.DataFrame({'a': ["fearlessly", "littleness", "lovingly", "devoted"]})
print(df.with_columns(b=pl.col('a').mp.snowball_stem()))