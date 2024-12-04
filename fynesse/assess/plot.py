import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns


def scatter(df, x_column, y_column, alpha=1):
    plt.scatter(df[x_column], df[y_column], alpha=alpha)
    plt.xlabel(x_column)
    plt.ylabel(y_column)
    plt.show()


def correlation_matrix(df):
    matrix = df.corr()
    sns.heatmap(matrix, annot=True, cmap="coolwarm", fmt=".2f")
    plt.show()


def heatmap(df, x, y, ylim=None):
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df, x=x, y=y, bins=(100, 100), cbar=True, cmap="Blues")
    if ylim is not None:
        plt.ylim(ylim[0], ylim[1])
    plt.xlabel(x)
    plt.ylabel(y)
    plt.show()


def heatmap_x_normalised(df, x, y, ylim):
    plt.figure(figsize=(10, 6))
    ymin, ymax = ylim

    x_bins = np.linspace(df[x].min(), df[x].max(), 100)
    y_bins = np.linspace(ymin, ymax, 100)

    hist, x_edges, y_edges = np.histogram2d(
            df[x],
            df[y],
            bins=[x_bins, y_bins]
    )

    column_totals = hist.sum(axis=1, keepdims=True)
    normalized_hist = hist / column_totals

    normalized_hist = np.nan_to_num(normalized_hist)

    plt.imshow(
            normalized_hist.T,
            origin='lower',
            aspect='auto',
            extent=[x_bins[0], x_bins[-1], y_bins[0], y_bins[-1]],
            cmap="Blues"
    )

    plt.colorbar(label=f"Proportion of {x}")
    plt.xlabel(x, fontsize=14)
    plt.ylabel(y, fontsize=14)
    plt.grid(False)

    plt.show()
