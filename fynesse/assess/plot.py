import matplotlib.pyplot as plt
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
