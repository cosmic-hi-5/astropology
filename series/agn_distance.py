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


if __name__ == "__main__":

    mp.set_start_method("spawn", force=True)

    start_time = time.perf_counter()

    # Configuration File
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    config_file_name = "agn_distance.ini"
    parser.read(f"{config_file_name}")

    # Load data
    data_directory = parser.get("directory", "data")
    data_name = parser.get("file", "data")

    # load name of files
    with open(f"{data_directory}/lcs.list", "r") as file:

        f_names = file.read()
        f_names = f_names.split("\n")
        f_names.remove("")

    # Get dictionary with time series
    number_series = parser.getint("config", "number_series")

    object_ids = np.empty(number_series)

    lcs = mp.Manager().dict()
    # lcs = {}

    for idx, f_name in enumerate(f_names):

        # f_name -> 1126067051866.dat

        object_id = int(f_name.split(".")[0])
        object_ids[idx] = object_id

        print(f"Load data of: {object_id}", end="\r")

        lc_data = np.loadtxt(f"{data_directory}/lcs/{f_name}")
        
        # 0-> mjd, 1 -> mag, 2-> mag_error
        lcs[idx] = lc_data[:, 1]

        if idx + 1 == number_series: break

    # build grid for parallel computations
    # "f" means float32
    matrix_distance = RawArray("f", number_series**2)

    x = np.arange(number_series)
    matrix_ij_grid = [(i, j) for i in x for j in x if j>i]

    distance = parser.get("config", "distance")
    print(f"Compute {distance} distances for {number_series} series")

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
        f"{parallel_finish_time-parallel_start_time:2f}"
    )

    save_to = parser.get("directory", "save_to")

    matrix_name = f"agn_{distance}_{number_series}_series"

    np.save(
        f"{data_directory}/{matrix_name}.npy",
        matrix_distance
    )

    np.save(
        f"{data_directory}/objid_{matrix_name}.npy",
        object_ids
    )


    finish_time = time.perf_counter()

    print(f"Running time: {finish_time - start_time:.2f}")
