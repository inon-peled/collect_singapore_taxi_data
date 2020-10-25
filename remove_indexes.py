import pandas as pd
from tqdm import tqdm
from glob import glob

from multiprocessing import Pool
import os


def remove_index(parquet_path):
    df = pd.read_parquet(parquet_path)
    print(parquet_path, len(df))
    if len(df) > 0:
        df.to_parquet(os.path.join('no_ts', os.path.basename(parquet_path)), index=False)


def remove_index_from_all_parquets():
    tasks = glob('./parquet/*/*.parquet')
#     with Pool(maxtasksperchild=1) as pool:
    for none_res in tqdm(map(remove_index, tasks), total=len(tasks)):
        pass

        
if __name__ == '__main__':        
    remove_index_from_all_parquets()