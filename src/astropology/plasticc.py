"""Utils to work with plasticc data and meta data"""

import numpy as np
import pandas as pd

from astropology.constants import PASSBANDS


def lcs_from_plasticc(raw_df: pd.DataFrame, passband: str) -> list[dict, dict]:

    """
    Obtain light curves and mjds of a specific passband in plasticc df.
    Light curves and mjds are stored in a dictionary, where the keys are
    the object_ids.

    INPUTS
    raw_df: data frame with plasticc light curves
    passband: name of passband, for instance: "u", "g"...

    OUTPUT
    lcs, mjds: light curves and mjds
    """

    passband_df = raw_df[raw_df["passband"] == PASSBANDS[passband]]
    lcs = {}
    mjds = {}

    unique_objid = np.unique(passband_df["object_id"])

    for object_id in unique_objid:

        id_mask = object_id == passband_df["object_id"]

        lcs[object_id] = passband_df.loc[id_mask, "flux"].to_numpy()

        mjds[object_id] = passband_df.loc[id_mask, "mjd"].to_numpy()

    return lcs, mjds


def rank_to_object_id(
    rank: int, score: np.array, idx_objid_map: np.array
) -> int:

    """
    Obtain the object_id of a light curve according to its anomalous
    rank. For instance, rank=-1 means the most anomalous light curve.

    INPUT
    rank: python like index to idicate rank of anomalous light curve.
        rank=0 is the least anomalous, while rank=-1 is the most
        anomalous.
    score: the anomaly score from which the rank is computed.
    idx_objid_map: array with map between the light curve index in the score array and the object_id of the light curve.

    OUTPUT
    object_id: unique identifier of the light curve.
    """

    score_arg_sort = np.argsort(score)

    idx_lc = score_arg_sort[rank]

    id_mask = idx_objid_map[:, 0] == idx_lc

    object_id = idx_objid_map[id_mask, 1][0]

    return object_id


def scores_of_target(
    target: int, score: np.array, meta_data: pd.DataFrame, idx_id_map: np.array
) -> np.array:
    """
    Extract anomaly scores of a particular target from the whole anomaly
    score array.
    """
    # get object_ids of the target class ()
    mask = meta_data["target"] == target
    object_ids = meta_data.loc[mask].index.to_numpy()

    # get scores of target class
    mask = np.isin(
        element=idx_id_map[:, 1], test_elements=object_ids, assume_unique=True
    )

    idxs = idx_id_map[mask, 0]

    score = score[idxs]

    return score
