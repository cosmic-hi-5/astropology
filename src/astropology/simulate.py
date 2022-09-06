"""Generate syntetic data to test concepts of TDA"""

from typing import Callable

import numpy as np

def time_series(
    signal_function: Callable,
    period: float,
    number_samples: int,
    variance: float,
    mean_dt: float,
    irregular_dt: float
):
    """
    Generate a time series according to input function
    from Xiangyu Jin

    INPUT:
    signal_function: function to generate signal, it must accept a
        time grid and a period as arguments  
    period: period of the signal
    number_samples: number of sample points
    variance: variance of the signal points
    mean_dt: mean sample time, in units of period
    irregular_dt: iregular sampling time [0,1]

    OUTPUT:
    t, signal, regular_grid:
        t: time grid of the signal
        signal: signal values
        regular_grid: whether or not the grid is regularly sampled

    """

    if (number_samples * mean_dt) < 1.:

        print('Warning: the signal lasts less than one period')

    dt = np.ones(number_samples)

    if irregular_dt == 0.0:

        regular_grid = True
        dt *= mean_dt * period

    else:

        dt += np.abs(
            np.random.normal(
                loc=0 ,scale=irregular_dt,
                size=number_samples
            )
        ) * mean_dt * period

        regular_grid = False
    
    t = np.cumsum(dt)

    signal = signal_function(t, period)
    
    signal += np.random.normal(
        loc=0, scale=np.sqrt(variance), size=number_samples
    )

    return t , signal, regular_grid 