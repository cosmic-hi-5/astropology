"""
Compute distance matrix between 1-D arrays such as time series of
spectra using cpu parallelization
"""

import multiprocessing as mp
from multiprocessing.sharedctypes import RawArray
import numpy as np
import sys

from astropology.distance import bottleneck_distance
from astropology.distance import wasserstein_distance
from astropology.series import pd_time_series

def raw_array_to_numpy(
    array: RawArray, array_shape: tuple=None
) -> np.array:

    """Create a numpy array backed by a shared memory Array."""

    array = np.ctypeslib.as_array(array)

    if array_shape is not None:

        return array.reshape(array_shape)

    return array


def share_data(
    shared_counter: mp.Value,
    shared_lcs: dict,
    shared_matrix: tuple,
):
    
    global lcs
    global counter
    global matrix_distance

    lcs = shared_lcs
    
    counter = shared_counter

    matrix_distance = raw_array_to_numpy(
        array=shared_matrix[0],
        array_shape= (shared_matrix[1], shared_matrix[1])
    )
    
def fill_distance_matrix(matrix_index: tuple, distance: str):
    
    with counter.get_lock():

        counter_value = counter.value

        counter.value += 1

        print(
            f"[{counter_value}] Compute matrix element: {matrix_index}",
            end="\r"
        )

    i = matrix_index[0]
    pdgm_i = pd_time_series(lcs[f"{i}"])

    j = matrix_index[1]
    pdgm_j = pd_time_series(lcs[f"{j}"])

    # remove point at infinity before computing distances
    pdgm_i = pdgm_i[np.isfinite(pdgm_i[:, 1]), :]
    pdgm_j = pdgm_j[np.isfinite(pdgm_j[:, 1]), :]

    if distance == "wasserstein":

        try: 
            
            d_ij = wasserstein_distance(pdgm_i, pdgm_j)

        except IndexError:

            print(f"IndexError at pdgms{matrix_index}")

            sys.exit()

    elif distance == "bottleneck":
        
        try:
            
            d_ij = bottleneck_distance(pdgm_i, pdgm_j)
        
        except IndexError:

            print(f"IndexError at pdgms{matrix_index}")

            sys.exit()
    
    matrix_distance[j, i] = d_ij
    matrix_distance[i, j] = d_ij
