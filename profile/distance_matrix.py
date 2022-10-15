import glob
import cProfile
import pstats

from memory_profiler import profile
import numpy as np

from astropology.distance import bottleneck_distance
from astropology.distance import wasserstein_distance
from astropology.signal import pd_from_distance_matrix
from astropology.signal import sparse_matrix_signal

def distance(
    signal_i: np.array, signal_j:np.array
)->tuple[float, float]:

    sparse_matrix_i = sparse_matrix_signal(signal_i)
    pdgm_i = pd_from_distance_matrix(sparse_matrix_i)

    sparse_matrix_j = sparse_matrix_signal(signal_j)
    pdgm_j = pd_from_distance_matrix(sparse_matrix_j)

    wd = wasserstein_distance(pdgm_i, pdgm_j)
    bd = bottleneck_distance(pdgm_i, pdgm_j)

@profile
def mem_distance(
    signal_i: np.array, signal_j:np.array
)->tuple[float, float]:

    sparse_matrix_i = sparse_matrix_signal(signal_i)
    pdgm_i = pd_from_distance_matrix(sparse_matrix_i)

    sparse_matrix_j = sparse_matrix_signal(signal_j)
    pdgm_j = pd_from_distance_matrix(sparse_matrix_j)

    wd = wasserstein_distance(pdgm_i, pdgm_j)
    bd = bottleneck_distance(pdgm_i, pdgm_j)

if __name__ == "__main__":

    lc_names = glob.glob(
        "/home/edgar/astropology/data/agn_lcs/lcs/*.dat"
    )[:2]

    lcs = []

    for idx, fname in enumerate(lc_names):

        lc = np.loadtxt(fname)

        lcs.append(lc[:, 1])
    
    mem_distance(lcs[0], lcs[1])

    with cProfile.Profile() as pr:

        distance(lcs[0], lcs[1])

    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    # stats.print_stats()
    stats.dump_stats(filename="/home/edgar/Downloads/distance.prof")