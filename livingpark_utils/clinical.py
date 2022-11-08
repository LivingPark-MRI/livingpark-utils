"""Computation of various clinical metrics for Parkinson's Disease."""
import math

import numpy as np


def moca2mmse(moca_score: int) -> int:
    """Return a MMSE score given a MoCA score.

    Parameters
    ----------
    moca_score: int
        MoCA score

    Returns
    -------
    int
        MMSE score corresponding to the MoCA score
        Conversion made using Table 2 in
        https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4371590

    """
    mapping = {
        1: 6,
        2: 9,
        3: 11,
        4: 12,
        5: 13,
        6: 14,
        7: 15,
        8: 15,
        9: 16,
        10: 17,
        11: 18,
        12: 18,
        13: 19,
        14: 20,
        15: 21,
        16: 22,
        17: 22,
        18: 23,
        19: 24,
        20: 25,
        21: 26,
        22: 26,
        23: 27,
        24: 28,
        25: 28,
        26: 29,
        27: 29,
        28: 30,
        29: 30,
        30: 30,
    }

    try:
        if math.isnan(moca_score):
            return np.nan
        else:
            return mapping[moca_score]
    except Exception as e:
        print(e)
        return moca_score
