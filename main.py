from decimal import Decimal, getcontext
from operator import itemgetter
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from functools import wraps

from http import HTTPStatus
import requests

BIKE_STATIONS = 'https://wegfinder.at/api/v1/stations'
#NEARBY_ADDRESS = 'https://api.i-mobility.at/routing/api/v1/nearby_address?latitude={latitude:}&longitude={longitude}'
NEARBY_ADDRESS = 'https://api.i-mobility.at/routing/api/v1/nearby_address'



def get_timing(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        exec_time = time.perf_counter() - start
        print(f'{func.__name__} executed {exec_time:.4f} s')
        return result

    return wrapper

def gen_free_bikes(bikes):
    """generator returns dictionary of free bikes"""
    prec = getcontext().prec
    getcontext().prec = 2
    for bike in bikes:
        if bike['free_bikes'] <= 0:
            continue
        new_bike = dict(bike)

        new_bike.pop('internal_id')
        status = new_bike.pop('status')
        longitude = new_bike.pop('longitude')
        latitude = new_bike.pop('latitude')

        new_bike['active'] = True if status == 'aktiv' else False
        new_bike['coordinates'] = longitude, latitude
        new_bike['free_ratio'] = float(str(Decimal(new_bike['free_boxes']) / Decimal(new_bike['boxes'])))

        yield new_bike

    getcontext().prec = prec


def get_free_citybikes():
    """loading, filterin, sorting bikes"""
    result = None
    resp = requests.get(BIKE_STATIONS)
    if resp.status_code == HTTPStatus.OK:
        bikes = sorted(gen_free_bikes(resp.json()), key=itemgetter('name'))
        bikes.sort(key=itemgetter('free_bikes'), reverse=True)
        result = bikes
    return result

def add_address(bike):
    """fetch address and append to dictionary"""
    longitude, latitude = bike['coordinates']
    params = {'longitude': longitude, 'latitude': latitude}
    resp = requests.get(NEARBY_ADDRESS, params=params)
    if resp.status_code == HTTPStatus.OK:
        body = resp.json()
        bike['address'] = body['data']['name']
    return resp.status_code

# single thread
def add_addresses(bikes):
    """fetch addresses and append to dictionary"""
    for bike in bikes:
        add_address(bike)

def run_bikes_by_executor(bikes):
    threads = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        for bike in bikes:
            threads.append(executor.submit(add_address, bike))

        for task in as_completed(threads):
            res = task.result()

@get_timing
def get_and_save_free_bikes(file_name='free_bikes.json'):
    bikes = get_free_citybikes()
    if bikes:
        #add_addresses(bikes)
        run_bikes_by_executor(bikes)
        with open(file_name, 'w') as json_file:
            json.dump(bikes, json_file, indent=4)


if __name__ == '__main__':
    get_and_save_free_bikes()