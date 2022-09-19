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

    start_time = time.perf_counter()
    
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

    if number_series != -1:
        
        object_ids = object_ids[: number_series]

    else:

        number_series = object_ids.size
    # Get light curves
    lcs = {}
    mjds = {}
    # map between index and object_id
    idx_objid = np.empty((number_series, 2))

    for idx, object_id in enumerate(object_ids):
        
        # idx_objid[f"{idx}"] = object_id
        idx_objid[idx, 0] = idx 
        idx_objid[idx, 1] = object_id

        id_mask = object_id == df['object_id']

        lcs[f"{idx}"] = df.loc[id_mask, "flux"].to_numpy()
        mjds[f"{idx}"] = df.loc[id_mask, "mjd"].to_numpy()

    # Some preprocessing

    # Normalize
    normalization = parser.get("config", "normalization")
    
    if normalization == "mean":

        for _, value in lcs.items(): value *= np.nanmean(value)
        

    elif normalization == "median":

        for _, value in lcs.items(): value *= np.nanmedian(value)

    elif normalization == "no":

        pass

    else:

        print(f"Normalization  {normalization} not defined")


    keep_negative = parser.getboolean("config", "keep_negative_flux")

    # Negative fluxes
    if keep_negative is False:

        for _, value in lcs.items(): value[value < 0] = 0

    # build grid for parallel computations
    # "f" means float32
    matrix_distance = RawArray("f", number_series**2)

    x = np.arange(number_series)
    matrix_ij_grid = [(i, j) for i in x for j in x if j>i]

    distance = parser.get("config", "distance")
    print(f"Compute {distance} distances for {number_series} series\n")

    worker = partial(fill_distance_matrix, distance=distance)

    number_jobs = parser.getint("config", "number_jobs")

    # add counter

    counter = mp.Value("i", 0)
    
    parallel_start_time = time.perf_counter()
    
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

    parallel_finish_time = time.perf_counter()

    print(
        f"\nPool of workers took:"
        f"{parallel_finish_time-parallel_start_time:2f}\n"
    )
    
    save_to = parser.get("directory", "save_to")

    matrix_name = (
        f"{distance}_series_{number_series}_norm_{normalization}"
        f"{band}_band"
    )

    if keep_negative is True:
        matrix_name = f"{matrix_name}_keep_negative"
    
    np.save(
        f"{data_directory}/{matrix_name}.npy",
        matrix_distance
    )

    np.save(
        f"{data_directory}/objid_{matrix_name}.npy",
        idx_objid
    )

        
    finish_time = time.perf_counter()

    print(f"\nRunning time: {finish_time - start_time:.2f}")
