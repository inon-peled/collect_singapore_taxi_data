import os
from functools import reduce 
from glob import glob

from tqdm import tqdm
import pandas as pd


def sum_up_grid(grid_dir):
    tasks =  glob(os.path.join(grid_dir, '*'))
    return reduce(lambda ser1, ser2: ser1.add(ser2, fill_value=0), 
                  tqdm(
                      (pd.read_parquet(f).set_index(['grid_lat', 'grid_lon']).squeeze() 
                      for f in tasks), 
                  total=len(tasks)))


if __name__ == '__main__':
    df_grid = sum_up_grid(grid_dir='./grid')
    print(df_grid.head())
    df_grid.to_csv('df_grid.csv', header=True)
