import glob

from memory_profiler import profile
import numpy as np

from astropology.distance import bottleneck_distance, wasserstein_distance
from astropology.signal import pd_signal

@profile
def list_pdgms(lcs):

    pdgms = []
    
    for lc in lcs:

        pdgm = pd_signal(lc)
        pdgms.append(pdgm)

    return pdgms 

@profile
def pair_wise_distance_matrix(diagrams: list, distance: str) -> np.array:

    n_rows = len(diagrams)
    distance_matrix = np.empty((n_rows, n_rows))
    
    for i, diagram_i in enumerate(diagrams):

        for j, diagram_j in enumerate(diagrams):
            
            if i > j:
                
                continue
                
            elif i == j:
                distance_matrix[j, i] = 0.
                
            else:
                # set matching to False to return the
                # wasserstein/bottlenec distance only

                if distance == "wasserstein":
                    
                    distance_ij = wasserstein_distance(
                        diagram_i, diagram_j, matching=False
                    )
                
                elif distance == "bottleneck":

                    distance_ij = bottleneck_distance(
                        diagram_i, diagram_j, matching=False
                    )

                else:
                    print(f"Not implemented {distance}")
                    sys.exit()

                distance_matrix[i, j] = distance_ij
                distance_matrix[j, i] = distance_ij
        
    return distance_matrix


if __name__ == "__main__":

    lc_names = glob.glob(
        "/home/edgar/astropology/data/agn_lcs/lcs/*.dat"
    )

    lcs = []

    for idx, fname in enumerate(lc_names):

        lc = np.loadtxt(fname)

        lcs.append(lc[:, 1])

        if idx == 10: break

    pdgms = list_pdgms(lcs)

    # _ = pair_wise_distance_matrix(pdgms, "bottleneck")
    _ = pair_wise_distance_matrix(pdgms, "wasserstein")
