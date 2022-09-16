"""
Compute distance matrix between 1-D arrays such as time series of
spectra using cpu parallelization
"""

import multiprocessing as mp
from multiprocessing.sharedctypes import RawArray
import numpy as np

from astropology.distance import pair_wise_distance_matrix
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
    shared_matrix:tuple
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
    pass