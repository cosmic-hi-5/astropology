"""Functionality persistence diagrams of time series"""

import numpy as np
from ripser import ripser
from scipy import sparse

def pd_from_distance_matrix(distance_matrix: sparse.coo_matrix):

    dgm_0 = ripser(
        distance_matrix, maxdim=0, distance_matrix=True
    )['dgms'][0]

    return dgm_0

def sparse_matrix_signal(signal: np.array)-> np.array:

    """
    Compute distance matrix asociated to the time serie.
    The distance is computed using the lower star filtration
    method.

    INPUT
    signal: time series measurements

    OUTPUT
    distance_matrix: sparse matrix of distances for the
        time serie using the lower star filtration method.
    """
    
    # define dimensionality of sparce matrix
    dimension = signal.size
    
    # from: https://github.com/ctralie/TDALabs
    # Add edges between adjacent points in the time series,
    # with the "distance" along the edge equal to the max 
    # value of the points it connects
    i = np.arange(dimension-1)
    j = np.arange(1, dimension)
    vertices = np.maximum(signal[0:-1], signal[1::])
    
    # Add vertex birth times along the diagonal of the
    # distance matrix
    i = np.concatenate((i, np.arange(dimension)))
    j = np.concatenate((j, np.arange(dimension)))
    vertices = np.concatenate((vertices, signal))
    #Create the sparse distance matrix
    distance_matrix = sparse.coo_matrix(
        (vertices, (i, j)),
        shape=(dimension, dimension)
    ).tocsr()
    
    return distance_matrix

def pd_signal(signal):
    
    distance_matrix = sparse_matrix_signal(signal)
    pd = pd_from_distance_matrix(distance_matrix)

    return pd
