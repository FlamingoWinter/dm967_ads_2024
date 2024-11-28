import matplotlib.pyplot as plt


def scatter(df, x_column, y_column, alpha=1):
    plt.scatter(df[x_column], df[y_column], alpha=alpha)
    plt.xlabel(x_column)
    plt.ylabel(y_column)
    plt.show()
