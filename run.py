import polars as pl
import minimal_plugin as mp

df = pl.DataFrame(
    {
        "a": [1, 3, 8],
        "b": [2.0, 3.1, 2.5],
        "c": ["3", "7", "3"],
    }
)

print(df.with_columns(mp.cum_sum('a')))

pl.Config().set_fmt_table_cell_list_len(10)

df = pl.DataFrame({'dense': [[0, 9], [8, 6, 0, 9], None, [3, 3]]})
print(df)
print(df.with_columns(indices=mp.non_zero_indices('dense')))

df = pl.DataFrame({'date': ['2024-02-01','2024-02-02','2024-02-03','2024-02-04','2024-02-05','2024-02-06','2024-02-07','2024-02-08','2024-02-09','2024-02-10'],
              'group':['A','A','A','A','A','B','B','B','B','B'],
              'value':[1,9,6,7,3, 2,4,5,1,9],
              'expected output':[None,0,1,2,1,None,0,0,1,0]})
print(df.with_columns(result = mp.distance_to_previous_greater_value('value').over('group')))
