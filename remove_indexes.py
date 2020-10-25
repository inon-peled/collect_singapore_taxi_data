from tqdm import tqdm
from glob import glob

from multiprocessing import Pool
import os


def remove_index(parquet_path):
    import pandas as pd
    try:
        df = pd.read_parquet(parquet_path)
        if len(df) > 0:
            df.to_parquet(os.path.join('no_ts', os.path.basename(parquet_path)), index=False, compression=None, engine='pyarrow')
    except Exception as e:
        print(parquet_path, e)


def remove_index_from_all_parquets():
    tasks = list(filter(lambda f: not os.path.exists('./no_ts/%s' % os.path.basename(f)), glob('./parquet/*/*.parquet')))
    with Pool(processes=64, maxtasksperchild=1) as pool:
        for none_res in tqdm(map(remove_index, tasks), total=len(tasks)):
            pass

        
if __name__ == '__main__':        
    remove_index_from_all_parquets()