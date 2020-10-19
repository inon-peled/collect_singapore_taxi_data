import requests

import gzip
from tqdm import tqdm
import random
import os
from multiprocessing import Pool
from time import sleep
from datetime import datetime, timedelta


def get_all(
        from_datetime_inclusive, 
        to_datetime_not_inclusive, 
        also_compress, 
        processes, 
        persist_dir, 
        min_sleep_seconds, 
        max_sleep_seconds):
    timestamps=[from_datetime_inclusive + timedelta(minutes=i) 
                for i in range(_minutes_diff(early=from_datetime_inclusive, late=to_datetime_not_inclusive))]
    with Pool(processes=processes, maxtasksperchild=1) as pool, open('download.log', 'a') as f_log:
        f_log.write('-------------- %s --------------\n' % datetime.now())
        tasks = _filter_out_already_downloaded(also_compress=also_compress, persist_dir=persist_dir, timestamps=timestamps)
        for timestamp, response in tqdm(
                pool.imap_unordered(_mp, 
                     ((also_compress, persist_dir, ts, random.randint(min_sleep_seconds, max_sleep_seconds)) for ts in tasks)),
                total=len(tasks)
        ):
            if not hasattr(response, 'content'):
                f_log.write('%s:\n%s\n' % (timestamp, response))
            elif response.status_code != requests.codes.ok:
                f_log.write('%s:\n%s\n' % (timestamp, response.content))

                
def _filter_out_already_downloaded(also_compress, persist_dir, timestamps):
    return list(filter(lambda ts: not os.path.exists(_fname(also_compress=also_compress, persist_dir=persist_dir, timestamp=ts)), 
                       timestamps))


def _fname(also_compress, persist_dir, timestamp):
    return os.path.join(persist_dir, 'sg_taxi_%s.json%s' % (timestamp.strftime('%Y%m%dT%H%M'), '.gz' if also_compress else ''))


def _one_ts(also_compress, persist_dir, timestamp):
    try:
        response = requests.get(url='https://api.data.gov.sg/v1/transport/taxi-availability?date_time=%s' %                            
                                    requests.utils.quote(timestamp.strftime('%Y-%m-%dT%H:%M:%S')),
                                headers={'accept': 'application/vnd.geo+json'})
        if response.status_code == requests.codes.ok:
            with (gzip.open if also_compress else open)(
                    _fname(also_compress=also_compress, 
                           persist_dir=persist_dir, 
                           timestamp=timestamp), 'wb') as f_out:
                f_out.write(response.content)
        return timestamp, response
    except Exception as e:
        return timestamp, e

    
def _minutes_diff(early, late):
    return int((late - early).total_seconds() / 60)


def _mp(args):
    also_compress, persist_dir, timestamp, sleep_seconds = args
    sleep(sleep_seconds)
    return _one_ts(also_compress=also_compress, persist_dir=persist_dir, timestamp=timestamp)

        
if __name__ == '__main__':
    get_all(
        from_datetime_inclusive=datetime(year=2017, month=1, day=1, hour=0, minute=0, second=0),
        to_datetime_not_inclusive=datetime(year=2018, month=1, day=1, hour=0, minute=0, second=0),
        also_compress=False,
        processes=64,
        min_sleep_seconds=1,
        max_sleep_seconds=5,
        persist_dir='2017'
    )
