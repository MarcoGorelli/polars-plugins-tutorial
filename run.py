import polars as pl
import minimal_plugin as mp


import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({
    'values': [[1, 3, 2], [5, 7], []],
    'weights': [[.5, .3, .2], [.1, .9], []]
})
print(df.with_columns(weighted_mean = mp.weighted_mean('values', 'weights')))

df = pl.DataFrame({
    'english': ['foo', 'bar', ''],
})
print(df.with_columns(pig_latin = mp.pig_latinnify('english')))

df = pl.DataFrame({
    'values': [1., 3, 2, 5, 7],
    'weights': [.5, .3, .2, .1, .9],
    'group': ['a', 'a', 'a', 'b', 'b'],
})
print(df.group_by('group').agg(weighted_mean = mp.vertical_weighted_mean('values', 'weights')))