"""
Functionality to compute distances between persistence diagrams
of time series data
"""

import sys

import numpy as np

from ripser import Rips
from persim import bottleneck ,wasserstein

def bottleneck_distance(
    diagram_i, diagram_j, matching: bool=False
) -> float:

    """
    Compute bottleneck distance between two persistence
    diagrams

    INPUT:
    diagram_i, diagram_j: input persistence diagrams 
        to compute wasserstein distance from 

    OUTPUT
    distance: bottleneck distance between input diagrams
    """

    distance = bottleneck(
        dgm1=diagram_i, dgm2=diagram_j, matching=matching
        )

    return distance


def wasserstein_distance(
    diagram_i, diagram_j, matching: bool=False
) -> float:

    """
    Compute wasserstein distance between two persistence
    diagrams

    INPUT:
    diagram_i, diagram_j: input persistence diagrams 
        to compute wasserstein distance from 

    OUTPUT
    distance: wasserstein distance between input diagrams
    """

    distance = wasserstein(
        dgm1=diagram_i, dgm2=diagram_j, matching=matching
        )

    return distance

def pair_wise_distance_matrix(diagrams: list, distance: str) -> np.array:

    """
    Compute pair wise distance matrix between all diagrams
    inputted to the fuction.

    INPUT
    diagrams: list with persistence diagrams
        e.g: [diagram_0, ...., diagram_n]
    distance: distance between persistence diagrams, can be
        Wassertein or Bottleneck
    
    OUTPUT
    distance_matrix: symmetric matrix with null diagonal
        where entry (i, j) is the wasserstein distance
        bewteen diagram_i and diagram_j 
    """

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