import pandas as pd

from fynesse.assess import predict


def show_feature_importance(df, features, predict_column, to_design_matrix):
    feature_importance = []

    for feature in features:
        new_features = [f for f in features if f != feature]
        r2_without, mse_without = predict.k_fold(df[new_features], df[predict_column],
                                                 to_design_matrix=to_design_matrix, report=False
                                                 )

        r2_only, mse_only = predict.k_fold(df[[feature]], df[predict_column],
                                           to_design_matrix=to_design_matrix, report=False
                                           )

        feature_importance.append({
                'feature'              : feature,
                'mean r2 wo feature'   : r2_without,
                'mean mse wo feature'  : mse_without,
                'mean r2 only feature' : r2_only,
                'mean mse only feature': mse_only,
        }
        )

    return pd.DataFrame(feature_importance)
