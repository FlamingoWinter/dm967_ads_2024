import matplotlib.pyplot as plt


def scatter(df, x_column, y_column):
    plt.scatter(df[x_column], df[y_column])
    plt.xlabel(x_column)
    plt.ylabel(y_column)
    plt.show()
