"Compute distance matrix time series data in parallel"

from configparser import ConfigParser, ExtendedInterpolation
from functools import partial
import multiprocessing as mp
from multiprocessing.sharedctypes import RawArray
import time

import numpy as np
import pandas as pd

from astropology.constants import PASSBANDS
from astropology.parrallel import fill_distance_matrix
from astropology.parrallel import raw_array_to_numpy
from astropology.parrallel import share_data

mean_norm = lambda flux: flux / np.nanmean(np.abs(flux))


def min_max_norm(flux: np.array):

    flux = (flux - np.nanmin(flux)) / (np.nanmax(flux) - np.nanmin(flux))

    return flux


f_normalization = {"mean": mean_norm, "max": max_norm, "min_max": min_max_norm}

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

    df = df.loc[df["passband"] == PASSBANDS[band]]

    object_ids = np.unique(df["object_id"])

    if number_series != -1:

        np.random.shuffle(object_ids)
        object_ids = object_ids[:number_series]

    else:

        number_series = object_ids.size
    # Get light curves
    lcs = {}
    # map between index and object_id
    idx_objid = np.empty((number_series, 2))

    for idx, object_id in enumerate(object_ids):

        # idx_objid[f"{idx}"] = object_id
        idx_objid[idx, 0] = idx
        idx_objid[idx, 1] = object_id

        id_mask = object_id == df["object_id"]

        lcs[idx] = df.loc[id_mask, "flux"].to_numpy()

    # Some preprocessing
    # Negative fluxes
    mask_negative = parser.getboolean("config", "mask_negative_flux")

    if mask_negative is True:

        for key, value in lcs.items():

            value = value[value >= 0]
            lcs[key] = value

    # Normalize
    normalization = parser.get("config", "normalization")

    if normalization != "no":

        assert normalization in f_normalization.keys()

        for key, value in lcs.items():

            value = f_normalization[normalization](value)
            lcs[key] = value

    # build grid for parallel computations
    # "f" means float32
    matrix_distance = RawArray("f", number_series ** 2)

    x = np.arange(number_series)
    matrix_ij_grid = [(i, j) for i in x for j in x if j > i]

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
        f"{distance}_series_{number_series}_norm_{normalization}_"
        f"{band}_band"
    )

    if mask_negative is True:
        matrix_name = f"{matrix_name}_mask_negative"

    np.save(f"{data_directory}/{matrix_name}.npy", matrix_distance)

    np.save(f"{data_directory}/objid_{matrix_name}.npy", idx_objid)

    finish_time = time.perf_counter()

    print(f"\nRunning time: {finish_time - start_time:.2f}")
