"""Collection of functions to perform visualization tasks."""
import pandas as pd
from matplotlib import axes


def reformat_plot_labels(dist: pd.Series, ax: axes.Axes, freq: int) -> None:
    """Reformat tick locations and labels of the x-axis on a plot.

    Parameters
    ----------
    dist: pd.Series
        Series representing the number of elements
        for each distinct values of a column
    ax: axes.Axes
        Matplotlib's Axes class to access figure
        elements and set the coordinate system
    freq: int
        interval between labels

    Returns
    -------
    None
    """
    ax.set_xticklabels([x.removesuffix(".0") for x in dist.index.astype(str)])
    for label in ax.xaxis.get_ticklabels():
        try:
            if int(label.get_text()) % freq != 0:
                label.set_visible(False)
        except Exception:
            pass
