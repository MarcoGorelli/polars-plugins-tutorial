import polars as pl
import minimal_plugin as mp

df = pl.DataFrame({'date': ['2024-02-01','2024-02-02','2024-02-03','2024-02-04','2024-02-05','2024-02-06','2024-02-07','2024-02-08','2024-02-09','2024-02-10'],
              'group':['A','A','A','A','A','B','B','B','B','B'],
              'value':[1,9,6,7,3, 2,4,5,1,9],
              'expected output':[None,0,1,2,1,None,0,0,1,0]})
print(df.with_columns(result = mp.distance_to_previous_greater_value('value').over('group')))
print(df.with_columns(result = mp.distance_to_previous_greater_value('value'))['result'].to_list())
