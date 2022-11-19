"""
Compute distance matrix between 1-D arrays such as time series of
spectra using cpu parallelization
"""

import multiprocessing as mp
from multiprocessing.sharedctypes import RawArray
import numpy as np

from astropology.distance import bottleneck_distance
from astropology.distance import wasserstein_distance
from astropology.signal import pd_signal


def raw_array_to_numpy(array: RawArray, array_shape: tuple = None) -> np.array:

    """Create a numpy array backed by a shared memory Array."""

    array = np.ctypeslib.as_array(array)

    if array_shape is not None:

        return array.reshape(array_shape)

    return array


def share_data(
    shared_counter: mp.Value,
    shared_lcs: dict,
    shared_matrix: tuple,
) -> None:
    """
    Share light curves among child processes durting distance
    matrix element wise computations
    """

    global lcs
    global counter
    global matrix_distance

    lcs = shared_lcs

    counter = shared_counter

    matrix_distance = raw_array_to_numpy(
        array=shared_matrix[0],
        array_shape=(shared_matrix[1], shared_matrix[1]),
    )


def fill_distance_matrix(matrix_index: tuple, distance: str) -> None:
    """
    Compute distance between signal i and j and fill in the
    distance matrix

    INPUT
    matrix_index: (i, j) element in the matrix distance
    distance: either the Wasserstein or Bottleneck distance
        between signal i and j
    """

    with counter.get_lock():

        counter_value = counter.value

        counter.value += 1

        print(
            f"[{counter_value}] Compute matrix element: {matrix_index}",
            end="\r",
        )

    i = matrix_index[0]
    pdgm_i = pd_signal(lcs[i])

    j = matrix_index[1]
    pdgm_j = pd_signal(lcs[j])

    if distance == "wasserstein":

        try:

            d_ij = wasserstein_distance(pdgm_i, pdgm_j)

        except IndexError:

            print(f"\nIndexError at pdgms: {matrix_index}\n")

            d_ij = np.nan

    elif distance == "bottleneck":

        try:

            d_ij = bottleneck_distance(pdgm_i, pdgm_j)

        except IndexError:

            print(f"\nIndexError at pdgms: {matrix_index}\n")

            d_ij = np.nan

    matrix_distance[j, i] = d_ij
    matrix_distance[i, j] = d_ij


def share_spectra(
    shared_counter: mp.Value,
    shared_spectra: tuple[str, RawArray, int, int],
    shared_matrix: tuple[RawArray, int],
) -> None:
    """
    Share light curves among child processes durting distance
    matrix element wise computations
    """

    global spectra
    global counter
    global matrix_distance

    spectra = raw_array_to_numpy(
        array=shared_spectra[1],
        array_shape=(shared_spectra[2], shared_spectra[3]),
    )

    spectra[...] = np.load(shared_spectra[0])

    counter = shared_counter

    matrix_distance = raw_array_to_numpy(
        array=shared_matrix[0],
        array_shape=(shared_matrix[1], shared_matrix[1]),
    )


def fill_spectra_distance(matrix_index: tuple, distance: str) -> None:
    """
    Compute distance between spectrum i and j and fill in the
    distance matrix

    INPUT
    matrix_index: (i, j) element in the matrix distance
    distance: either the Wasserstein or Bottleneck distance
        between signal i and j
    """
    with counter.get_lock():

        counter_value = counter.value

        counter.value += 1

        print(
            f"[{counter_value}] Compute matrix element: {matrix_index}",
            end="\r",
        )

    i = matrix_index[0]
    pdgm_i = pd_signal(spectra[i])

    j = matrix_index[1]
    pdgm_j = pd_signal(spectra[j])

    if distance == "wasserstein":

        try:

            d_ij = wasserstein_distance(pdgm_i, pdgm_j)

        except IndexError:

            print(f"\nIndexError at pdgms: {matrix_index}\n")

            d_ij = np.nan

    elif distance == "bottleneck":

        try:

            d_ij = bottleneck_distance(pdgm_i, pdgm_j)

        except IndexError:

            print(f"\nIndexError at pdgms: {matrix_index}\n")

            d_ij = np.nan

    matrix_distance[j, i] = d_ij
    matrix_distance[i, j] = d_ij
