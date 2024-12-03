import osmnx as ox
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


def amend_df_with_indicators(df,
                             distance_km: float = 1.0,
                             lat_column_name="lat",
                             lng_column_name="lng",
                             geometry_column_name="geometry",
                             radius_around_point=False
                             ):
    def count_pois(row):
        tags = {
                "building"      : ["dormitory", "university", "academic", "office", "residential"],
                "leisure"       : ["playground", "park"],
                "amenity"       : ["bench", "fast_food", "school", "nightclub", "pub", "cafe",
                                   "theatre", "cinema"],
                "residential"   : ["retirement_home"],
                "shop"          : ["books", "clothes"],
                "toilets:unisex": ["yes"]
        }

        columns = {
                "houses"              : {"building": "residential"},

                "dorms"               : {"building": "dormitory"},
                "university_buildings": {"building": "university"},
                "academic_buildings"  : {"building": "academic"},
                "office_buildings"    : {"building": "office"},

                "playgrounds"         : {"leisure": "playground"},
                "benches"             : {"amenity": "bench"},
                "retirement_homes"    : {"residential": "retirement_home"},
                "fast_foods"          : {"amenity": "fast_food"},
                "schools"             : {"amenity": "school"},
                "clubs"               : {"amenity": "nightclub"},
                "pubs"                : {"amenity": "pub"},
                "parks"               : {"leisure": "park"},

                "cafes"               : {"amenity": "cafe"},
                "theatres"            : {"amenity": "theatre"},
                "cinemas"             : {"amenity": "cinema"},
                "bookshops"           : {"shop": "books"},
                "fashion_shops"       : {"shop": "clothes"},
                "unisex_toilets"      : {"toilets:unisex": "yes"}
        }

        to_amend = {}

        try:
            if radius_around_point:
                lat, lng = row[lat_column_name], row[lng_column_name]
                pois = ox.features_from_point((lat, lng), tags, dist=distance_km * 1000)
            else:
                polygon = row[geometry_column_name]
                pois = ox.features_from_polygon(polygon, tags)

            for column_name, tags in columns.items():
                filtered = pois
                for tag_key, tag_value in tags.items():
                    if tag_key in filtered.columns:
                        filtered = filtered[filtered[tag_key] == tag_value]
                    else:
                        filtered = pd.DataFrame()

                to_amend[column_name] = len(filtered)

        except:
            # InsufficientResponseError
            for column_name in columns:
                to_amend[column_name] = 0

        return pd.Series(to_amend)

    pois_counts = df.apply(count_pois, axis=1)

    df = pd.concat([df, pois_counts], axis=1)

    return df
