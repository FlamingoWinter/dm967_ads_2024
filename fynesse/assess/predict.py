import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.model_selection import KFold
from sklearn.preprocessing import StandardScaler


def fit_linear_model(xs, y, alpha=0, L1_wt=0, feature_names=None):
    def to_design_matrix(xs):
        return xs

    m = sm.OLS(y, xs)

    scaler = StandardScaler()
    xs_normalised = scaler.fit_transform(xs)
    xs_design = to_design_matrix(xs_normalised)
    m = sm.OLS(y, xs_design)
    fit_m = m.fit_regularized(alpha=alpha, L1_wt=L1_wt)

    coefficients = fit_m.params
    unnormalised_coefficients = coefficients / scaler.scale_

    print("Linear Model Fitted with Features:")
    coefficients_df = pd.DataFrame({
            "Feature"                : feature_names,
            "Unormalised_Coefficient": unnormalised_coefficients
    }
    )

    print(coefficients_df)
    fit_m.scaler = scaler
    fit_m.to_design_matrix = to_design_matrix
    return fit_m


def fit_linear_model_with_design_matrix(xs, y, alpha=0, L1_wt=0, feature_names=None,
                                        to_design_matrix=None
                                        ):
    def default_to_design_matrix(xs):
        intercept = np.ones((xs.shape[0], 1))
        return np.hstack([intercept, xs])

    if to_design_matrix is None:
        to_design_matrix = default_to_design_matrix

    scaler = StandardScaler()
    xs_normalised = scaler.fit_transform(xs)
    xs_design = to_design_matrix(xs_normalised)
    m = sm.OLS(y, xs_design)
    fit_m = m.fit_regularized(alpha=alpha, L1_wt=L1_wt)

    coefficients = fit_m.params

    fit_m.scaler = scaler
    fit_m.to_design_matrix = to_design_matrix
    return fit_m


def predict(model, xs):
    xs_normalised = model.scaler.transform(xs)
    xs_design = model.to_design_matrix(xs_normalised)
    return model.predict(xs_design)


def k_fold(xs, y, alpha=0, L1_wt=0, n_splits=10, to_design_matrix=None):
    kf = KFold(n_splits=n_splits, shuffle=True)
    mses = []
    r2s = []

    for train_index, test_index in kf.split(xs):
        xs_train, xs_test = xs.iloc[train_index], xs.iloc[test_index]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]

        model = fit_linear_model_with_design_matrix(xs_train, y_train, alpha=alpha, L1_wt=L1_wt,
                                                    to_design_matrix=to_design_matrix
                                                    )

        predictions = predict(model, xs_test)

        r2 = r2_score(y_test, predictions)

        mse = mean_squared_error(y_test, predictions)
        mses.append(mse)
        r2s.append(r2)

    avg_r2 = np.mean(r2s)
    print(f"Average r2: {avg_r2}")
    avg_mse = np.mean(mses)
    print(f"Average mse: {avg_mse}")
