"Compute distance matrix time series data in parallel"

from configparser import ConfigParser, ExtendedInterpolation
from functools import partial
import multiprocessing as mp
from multiprocessing.sharedctypes import RawArray
import time

import numpy as np
import pandas as pd

from astropology.parrallel import fill_spectra_distance
from astropology.parrallel import raw_array_to_numpy
from astropology.parrallel import share_spectra


if __name__ == "__main__":

    mp.set_start_method("spawn", force=True)

    start_time = time.perf_counter()

    # Configuration File
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    config_file_name = "sdss_distance.ini"
    parser.read(f"{config_file_name}")

    print("Load data")

    data_directory = parser.get("directory", "data")
    data_name = parser.get("file", "data")

    # share array of spectra
    number_spectra, number_waves = np.load(
        f"{data_directory}/{data_name}", mmap_mode="r"
    ).shape

    parser_n_spectra = parser.getint("config", "number_spectra")

    if parser_n_spectra != -1:

        number_spectra = parser_n_spectra

    print("Set shared memory")

    # "f" means float32
    spectra = RawArray("f", number_spectra * number_waves)

    # build grid for parallel computations
    matrix_distance = RawArray("f", number_spectra ** 2)

    x = np.arange(number_spectra)
    matrix_ij_grid = [(i, j) for i in x for j in x if j > i]

    distance = parser.get("config", "distance")
    print(f"Compute {distance} distances for {number_spectra} series")

    worker = partial(fill_spectra_distance, distance=distance)

    number_jobs = parser.getint("config", "number_jobs")

    # add counter

    counter = mp.Value("i", 0)

    print("Start pool of workers")

    parallel_start_time = time.perf_counter()

    with mp.Pool(
        processes=number_jobs,
        initializer=share_spectra,
        initargs=(
            counter,
            (
                f"{data_directory}/{data_name}",
                spectra,
                number_spectra,
                number_waves,
            ),
            (matrix_distance, number_spectra),
        ),
    ) as pool:

        pool.map(worker, matrix_ij_grid)

    matrix_distance = raw_array_to_numpy(
        matrix_distance, (number_spectra, number_spectra)
    )

    parallel_finish_time = time.perf_counter()

    print(
        f"\nPool of workers took:"
        f"{parallel_finish_time-parallel_start_time:2f}"
    )

    save_to = parser.get("directory", "save_to")

    matrix_name = f"sdss_{distance}_{number_spectra}"

    np.save(f"{data_directory}/{matrix_name}.npy", matrix_distance)

    finish_time = time.perf_counter()

    print(f"Running time: {finish_time - start_time:.2f}")
