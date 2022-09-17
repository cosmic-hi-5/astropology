"Compute distance matrix time series data in parallel"
from configparser import ConfigParser, ExtendedInterpolation
from functools import partial
import multiprocessing as mp
from multiprocessing.sharedctypes import RawArray
import time

import numpy as np
import pandas as pd

from astropology.parrallel import fill_distance_matrix
from astropology.parrallel import raw_array_to_numpy
from astropology.parrallel import share_data


PASSBANDS = {"u":0, "g":1, "r":2, "i":3, "z":4, "y":5}

if __name__ == "__main__":

    mp.set_start_method("spawn", force=True)

    start_time = time.time()
    
    # Configuration File
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    config_file_name = "distance_parallel.ini"
    parser.read(f"{config_file_name}")
    
    # Load data
    data_directory = parser.get("directory", "data")
    data_name = parser.get("file", "data")
    
    df = pd.read_csv(f"{data_directory}/{data_name}")


    # Get dictionary with time series
    band = parser.get("config", "band")
    number_series = parser.getint("config", "number_series")

    df = df.loc[df["passband"]==PASSBANDS[band]]

    object_ids = np.unique(df["object_id"])

    if isinstance(number_series, int):
        
        object_ids = object_ids[: number_series]

    # Get light curves
    lcs = {}
    mjds = {}
    # map between index and object_id
    idx_objid = {}

    for idx, object_id in enumerate(object_ids):
        
        idx_objid[f"{idx}"] = object_id

        id_mask = object_id == df['object_id']

        lcs[f"{idx}"] = df.loc[id_mask, "flux"].to_numpy()
        mjds[f"{idx}"] = df.loc[id_mask, "mjd"].to_numpy()

    keep_negative = parser.getboolean("config", "keep_negative_flux")

    if keep_negative is False:

        for _, value in lcs.items(): value[value < 0] = 0


    # build grid for parallel computations    
    matrix_distance = RawArray("d", number_series**2)

    x = np.arange(number_series)
    matrix_ij_grid = [(i, j) for i in x for j in x if j>i]

    distance = parser.get("config", "distance")

    worker = partial(fill_distance_matrix, distance=distance)

    number_jobs = parser.getint("config", "number_jobs")

    # add counter

    counter = mp.Value("i", 0)

    with mp.Pool(
        processes=number_jobs,
        initializer=share_data,
        initargs=(
            counter,
            lcs,
            (matrix_distance, number_series),
        ),
    ) as pool:

        pool.map(worker, matrix_ij_grid)

    matrix_distance = raw_array_to_numpy(
        matrix_distance, (number_series, number_series)
    )

    np.save(f"{data_directory}/{distance}_matrix.npy", matrix_distance)

        
    finish_time = time.time()

    print(f"\nRunning time: {finish_time - start_time:.2f}")
