import random
from typing import Union

import matplotlib.pyplot as plt
import numpy as np
import osmnx as ox
import pandas as pd
import seaborn as sns
from ipywidgets import interact, widgets

from fynesse.assess import predict
from fynesse.common.db.db import run_query
from fynesse.common.db.db_index_management import add_index


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
            'feature': feature,
            'mean r2 wo feature': r2_without,
            'mean mse wo feature': mse_without,
            'mean r2 only feature': r2_only,
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
            "building": ["dormitory", "university", "academic", "office", "residential"],
            "leisure": ["playground", "park"],
            "amenity": ["bench", "fast_food", "school", "nightclub", "pub", "cafe",
                        "theatre", "cinema"],
            "residential": ["retirement_home"],
            "shop": ["books", "clothes"],
            "toilets:unisex": ["yes"]
        }

        columns = {
            "houses": {"building": "residential"},

            "dorms": {"building": "dormitory"},
            "university_buildings": {"building": "university"},
            "academic_buildings": {"building": "academic"},
            "office_buildings": {"building": "office"},

            "playgrounds": {"leisure": "playground"},
            "benches": {"amenity": "bench"},
            "retirement_homes": {"residential": "retirement_home"},
            "fast_foods": {"amenity": "fast_food"},
            "schools": {"amenity": "school"},
            "clubs": {"amenity": "nightclub"},
            "pubs": {"amenity": "pub"},
            "parks": {"leisure": "park"},

            "cafes": {"amenity": "cafe"},
            "theatres": {"amenity": "theatre"},
            "cinemas": {"amenity": "cinema"},
            "bookshops": {"shop": "books"},
            "fashion_shops": {"shop": "clothes"},
            "unisex_toilets": {"toilets:unisex": "yes"}
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


def sample_near_beaches(connection, sample_size):
    add_index(connection, "postcode_near_beach", "postcode_id")
    add_index(connection, "postcode_near_beach", "beach_id")

    count = run_query(connection, f"SELECT COUNT(*) FROM postcode").iloc[0, 0]
    sample_indexes = random.sample(range(count), sample_size)

    run_query(connection, f"DROP Table IF EXISTS sample_ids;")

    run_query(connection, "CREATE TEMPORARY TABLE sample_ids (id INT);")

    run_query(connection, "INSERT INTO sample_ids (id) VALUES (%s);",
              [(i,) for i in sample_indexes], execute_many=True
              )

    sample = run_query(connection,
                       f"""SELECT p.postcode, pb.distance_to_beach, h.price, h.date_of_transfer, b.name, h.primary_addressable_object_name, h.secondary_addressable_object_name
                        FROM sample_ids s
                        JOIN postcode p ON p.id = s.id
                        JOIN price_paid h ON p.id = h.db_id
                        JOIN postcode_near_beach pb ON p.id = pb.postcode_id
                        JOIN beach b ON pb.beach_id = b.id"""
                       )

    run_query(connection, "DROP TABLE sample_ids;")

    return sample


def sample_not_near_beaches(connection, sample_size):
    count = run_query(connection, f"SELECT COUNT(*) FROM postcode").iloc[0, 0]
    sample_indexes = random.sample(range(count), sample_size)

    run_query(connection, f"DROP Table IF EXISTS sample_ids;")

    run_query(connection, "CREATE TEMPORARY TABLE sample_ids (id INT);")

    run_query(connection, "INSERT INTO sample_ids (id) VALUES (%s);",
              [(i,) for i in sample_indexes], execute_many=True
              )

    not_beach_houses = run_query(connection,
                                 f"""
    SELECT p.postcode, h.price, h.date_of_transfer, h.primary_addressable_object_name, h.secondary_addressable_object_name
    FROM sample_ids s
    JOIN postcode p ON p.id = s.id
    JOIN price_paid h ON p.id = h.db_id
    LEFT JOIN postcode_near_beach pb ON p.id = pb.postcode_id
    WHERE pb.postcode_id IS NULL;
                                       """
                                 )
    run_query(connection, "DROP TABLE sample_ids;")

    return not_beach_houses


def render_interactive_distribution_graph(df_beach, df_not_beach):
    def update_kde(distance_range):
        min_distance, max_distance = distance_range - 50, distance_range
        filtered_data = df_beach[
            (df_beach['distance_to_beach'] >= min_distance) & (
                    df_beach['distance_to_beach'] <= max_distance)
            ]['price']

        plt.figure(figsize=(10, 6))

        if len(filtered_data) > 1:
            sns.kdeplot(filtered_data, fill=True, color="blue", alpha=0.5, gridsize=2000)
            sns.kdeplot(df_not_beach['price'], fill=True, color="blue", alpha=0.5,
                        gridsize=2000
                        )
            plt.title(f"Distance to Beach: {min_distance}m - {max_distance}m)")
            plt.xlabel("Price")
            plt.xlim(0, 2_000_000)
            plt.ylabel("Density")
            plt.grid(True)
            plt.ylim(0, 8e-6)
        else:
            plt.text(0.5, 0.5, "No Data", horizontalalignment='center', verticalalignment='center')
        plt.show()

    range_slider = widgets.IntSlider(value=(500), min=50, max=1000, step=50,
                                     description='distance_to_beach'
                                     )

    interact(update_kde, distance_range=range_slider)


def msoa_to_proportions(msoas):
    job_columns = [col for col in msoas.columns if col[0].isdigit()]
    hours_columns = [col for col in msoas.columns if col.startswith("hours_") and any(char.isdigit() for char in col)]
    dist_columns = [col for col in msoas.columns if col.startswith("dist_") and col != "dist_total_all_usual_residents"]

    msoas['job_total'] = msoas[job_columns].sum(axis=1)

    msoas[job_columns] = msoas[job_columns].div(msoas['job_total'], axis=0)
    msoas[hours_columns] = msoas[hours_columns].div(msoas['dist_total_all_usual_residents'], axis=0)
    msoas[dist_columns] = msoas[dist_columns].div(msoas['hours_total_all_usual_residents'], axis=0)
    return msoas


def oa_to_proportions(oas):
    hours_columns = [col for col in oas.columns if col.startswith("hours_") and any(char.isdigit() for char in col)]
    dist_columns = [col for col in oas.columns if col.startswith("dist_") and col != "dist_total_all_usual_residents"]
    job_columns = ['managers_directors_senior_officials', 'professional', 'associate_professional_technical',
                   'administrative_secretarial', 'skilled_trades',
                   'caring_leisure_other_service', 'sales_customer_service', 'process_plant_machine_operatives',
                   'elementary']

    oas['job_total'] = oas[job_columns].sum(axis=1)

    oas[job_columns] = oas[job_columns].div(oas['job_total'], axis=0)
    oas[hours_columns] = oas[hours_columns].div(oas['dist_total_all_usual_residents'], axis=0)
    oas[dist_columns] = oas[dist_columns].div(oas['hours_total_all_usual_residents'], axis=0)
    return oas


def get_areas_with_and_without_beaches(connection, level: Union["oa", "msoa"]):
    areas = run_query(connection, f"""
    SELECT
        *
    FROM {level} a
    LEFT JOIN beach_intersects_{level} b ON a.id = b.{level}_id;
    """)

    areas_with_beach = areas[areas['beach_id'].notnull()]
    areas_with_beach = areas_with_beach.groupby('Geography_Code').first().reset_index()

    areas_without_beach = areas[areas['beach_id'].isnull()]
    areas_without_beach = areas_without_beach.groupby('Geography_Code').first().reset_index()
    if level == "oa":
        return oa_to_proportions(areas_with_beach), oa_to_proportions(areas_without_beach)
    if level == "msoa":
        return msoa_to_proportions(areas_with_beach), msoa_to_proportions(areas_without_beach)
    return None


def plot_jobs_with_and_without_beach_msoa(msoas_with_beach, msoas_without_beach, job_columns):
    job_columns = [col for col in msoas_with_beach.columns if col[0].isdigit()]

    msoas_without_beach_averages = msoas_without_beach[job_columns].mean()
    msoas_with_beach_averages = msoas_with_beach[job_columns].mean()
    batch_size = 10
    num_batches = len(job_columns) // batch_size + (1 if len(job_columns) % batch_size != 0 else 0)

    fig, axes = plt.subplots(num_batches, 1, figsize=(12, 64))

    for batch_idx in range(num_batches):
        start = batch_idx * batch_size
        end = min(start + batch_size, len(job_columns))
        batch_columns = job_columns[start:end]

        x = np.arange(len(batch_columns))
        ax = axes[batch_idx]

        width = 0.35
        ax.bar(x - width / 2, msoas_with_beach_averages[batch_columns], width, label="With Beaches", color="skyblue")

        ax.bar(x + width / 2, msoas_without_beach_averages[batch_columns], width, label="Without Beaches",
               color="orange")
        ax.set_ylim(0, 0.07)
        ax.set_xticks(x)
        ax.set_xticklabels(batch_columns, rotation=45, ha="right")
        ax.set_ylabel("Proportion")
        ax.legend()
        plt.tight_layout()


def plot_job_info_with_and_without_beach_msoa(msoas_with_beach, msoas_without_beach):
    hours_columns = [col for col in msoas_with_beach.columns if
                     col.startswith("hours_") and any(char.isdigit() for char in col)]
    dist_columns = [col for col in msoas_with_beach.columns if
                    col.startswith("dist_") and col != "dist_total_all_usual_residents"]
    msoas_without_beach_averages = msoas_without_beach[hours_columns + dist_columns].mean()
    msoas_with_beach_averages = msoas_with_beach[hours_columns + dist_columns].mean()
    fig, axes = plt.subplots(2, 1, figsize=(35, 20))

    widths = [15, 15, 18, 30]
    xticks = []
    for n, c in enumerate(widths):
        xticks.append(sum(widths[:n]) + widths[n] / 2)

    widths = np.array(widths) * 0.4

    axes[0].bar(np.array(xticks) - np.array(widths) / 2, np.array(msoas_with_beach_averages[hours_columns]) / widths,
                np.array(widths), label="With Beaches", color="skyblue")
    axes[0].bar(np.array(xticks) + np.array(widths) / 2, np.array(msoas_without_beach_averages[hours_columns]) / widths,
                np.array(widths), label="Without Beaches", color="orange")
    axes[0].set_xticks(xticks, ["15 or less", "16 to 30", "31 to 48", "49+"])
    axes[0].set_title("Hours Worked")
    axes[0].set_yticks([])

    widths = [2, 3, 5, 10, 10, 10, 20, 30, 10, 10]
    xticks = []
    for n, c in enumerate(widths):
        xticks.append(sum(widths[:n]) + widths[n] / 2)
    widths = np.array(widths) * 0.4
    axes[1].bar(np.array(xticks) - np.array(widths) / 2, np.array(msoas_with_beach_averages[dist_columns]) / widths,
                np.array(widths), label="With Beaches", color="skyblue")
    axes[1].bar(np.array(xticks) + np.array(widths) / 2, np.array(msoas_without_beach_averages[dist_columns]) / widths,
                np.array(widths), label="Without Beaches", color="orange")
    axes[1].set_xticks(xticks,
                       ["<2km", "2km-5km", "5km-10km", "10km-20km", "20km-30km", "30km-40km", "40km-60km", "60km>",
                        "from home", "outside uk"])
    axes[1].set_title("Distance travelled")
    axes[1].set_yticks([])


def plot_jobs_with_and_without_beach_oa(oas_with_beach, oas_without_beach):
    fig, ax = plt.subplots(1, 1, figsize=(16, 8))

    job_columns = ['managers_directors_senior_officials', 'professional', 'associate_professional_technical',
                   'administrative_secretarial', 'skilled_trades',
                   'caring_leisure_other_service', 'sales_customer_service', 'process_plant_machine_operatives',
                   'elementary']
    oas_without_beach_averages = oas_without_beach[job_columns].mean()
    oas_with_beach_averages = oas_with_beach[job_columns].mean()
    width = 0.4
    x = np.arange(len(job_columns))

    ax.bar(x - width / 2, oas_with_beach_averages[job_columns], width, label="With Beaches", color="skyblue")

    ax.bar(x + width / 2, oas_without_beach_averages[job_columns], width, label="Without Beaches", color="orange")
    ax.set_xticks(x)
    ax.set_xticklabels(job_columns, rotation=45, ha="right")
    ax.set_ylabel("Count")
    ax.legend()
    plt.tight_layout()


def plot_job_info_with_and_without_beach_msoa(oas_with_beach, oas_without_beach):
    fig, axes = plt.subplots(2, 1, figsize=(35, 20))
    hours_columns = [col for col in oas_with_beach.columns if
                     col.startswith("hours_") and any(char.isdigit() for char in col)]
    dist_columns = [col for col in oas_with_beach.columns if
                    col.startswith("dist_") and col != "dist_total_all_usual_residents"]
    oas_without_beach_averages = oas_without_beach[hours_columns + dist_columns].mean()
    oas_with_beach_averages = oas_with_beach[hours_columns + dist_columns].mean()

    widths = [15, 15, 18, 30]
    xticks = []
    for n, c in enumerate(widths):
        xticks.append(sum(widths[:n]) + widths[n] / 2)

    widths = np.array(widths) * 0.4

    axes[0].bar(np.array(xticks) - np.array(widths) / 2, np.array(oas_with_beach_averages[hours_columns]) / widths,
                np.array(widths), label="With Beaches", color="skyblue")
    axes[0].bar(np.array(xticks) + np.array(widths) / 2, np.array(oas_without_beach_averages[hours_columns]) / widths,
                np.array(widths), label="Without Beaches", color="orange")
    axes[0].set_xticks(xticks, ["15 or less", "16 to 30", "31 to 48", "49+"])
    axes[0].set_title("Hours Worked")
    axes[0].set_yticks([])

    widths = [2, 3, 5, 10, 10, 10, 20, 30, 10, 10]
    xticks = []
    for n, c in enumerate(widths):
        xticks.append(sum(widths[:n]) + widths[n] / 2)
    widths = np.array(widths) * 0.4
    axes[1].bar(np.array(xticks) - np.array(widths) / 2, np.array(oas_with_beach_averages[dist_columns]) / widths,
                np.array(widths), label="With Beaches", color="skyblue")
    axes[1].bar(np.array(xticks) + np.array(widths) / 2, np.array(oas_without_beach_averages[dist_columns]) / widths,
                np.array(widths), label="Without Beaches", color="orange")
    axes[1].set_xticks(xticks,
                       ["<2km", "2km-5km", "5km-10km", "10km-20km", "20km-30km", "30km-40km", "40km-60km", "60km>",
                        "from home", "outside uk"])
    axes[1].set_title("Distance travelled")
    axes[1].set_yticks([])


def plot_hours_worked_by_detailed_work_type(connection):
    df = run_query(connection, "SELECT * FROM hours_worked_by_detailed_work_type")

    df["est_average_hours"] = (df["hours_31_to_48"] * 39 + df["hours_49_plus"] * 55 + df["hours_0_to_15"] * 7.5 + df[
        "hours_16_to_30"] * 22.5) / (
                                      df["hours_31_to_48"] + df["hours_49_plus"] + df["hours_0_to_15"] + df[
                                  "hours_16_to_30"])

    width = 0.9

    batch_size = 20
    num_batches = (len(df) - 1) // batch_size + (1 if (len(df) - 1) % batch_size != 0 else 0)

    fig, axes = plt.subplots(num_batches, 1, figsize=(24, 60))

    for batch_idx in range(num_batches):
        start = batch_idx * batch_size
        end = min(start + batch_size, len(df) - 1)
        rows = df[1 + start:1 + end]

        x = np.arange(len(rows))
        ax = axes[batch_idx]

        ax.bar(x, rows["est_average_hours"], width, label="With Beaches", color="skyblue")

        ax.set_ylim(0, 45)
        ax.set_xticks(x)
        ax.set_xticklabels(rows["occupation"], rotation=45, ha="right")
        ax.set_ylabel("Average Hours")
        ax.legend()
        plt.tight_layout()


def plot_distance_travelled_by_detailed_work_type(connection):
    df = run_query(connection, "SELECT * FROM hours_worked_by_detailed_work_type")

    df['est_average_distance'] = (
                                         df['distance_less_than_5km'] * 2.5 +
                                         df['distance_5_to_10km'] * 7.5 +
                                         df['distance_10_to_30km'] * 20 +
                                         df['distance_30_to_60km'] * 45 +
                                         df['distance_60km_plus'] * 75 +
                                         df['distance_works_from_home'] * 0 +
                                         df['distance_works_offshore_or_abroad'] * 100
                                 ) / (
                                         df['distance_less_than_5km'] +
                                         df['distance_5_to_10km'] +
                                         df['distance_10_to_30km'] +
                                         df['distance_30_to_60km'] +
                                         df['distance_60km_plus'] +
                                         df['distance_works_from_home'] +
                                         df['distance_works_offshore_or_abroad']
                                 )

    width = 0.9

    batch_size = 20
    num_batches = (len(df) - 1) // batch_size + (1 if (len(df) - 1) % batch_size != 0 else 0)

    fig, axes = plt.subplots(num_batches, 1, figsize=(24, 60))

    for batch_idx in range(num_batches):
        start = batch_idx * batch_size
        end = min(start + batch_size, len(df) - 1)
        rows = df[1 + start:1 + end]

        x = np.arange(len(rows))
        ax = axes[batch_idx]

        ax.bar(x, rows["est_average_distance"], width, label="With Beaches", color="skyblue")

        ax.set_ylim(0, 80)
        ax.set_xticks(x)
        ax.set_xticklabels(rows["occupation"], rotation=45, ha="right")
        ax.set_ylabel("Average Distance")
        ax.legend()
        plt.tight_layout()


def plot_hours_worked_by_work_type(connection):
    df = run_query(connection, "SELECT * FROM hours_worked_by_work_type")

    df["est_average_hours"] = (df["hours_31_to_48"] * 39 + df["hours_49_plus"] * 55 + df["hours_0_to_15"] * 7.5 + df[
        "hours_16_to_30"] * 22.5) / (
                                      df["hours_31_to_48"] + df["hours_49_plus"] + df["hours_0_to_15"] + df[
                                  "hours_16_to_30"])

    job_columns = ['managers_directors_senior_officials', 'professional', 'associate_professional_technical',
                   'administrative_secretarial', 'skilled_trades',
                   'caring_leisure_other_service', 'sales_customer_service', 'process_plant_machine_operatives',
                   'elementary']

    fig, axes = plt.subplots(1, 1, figsize=(24, 8))

    x = np.arange(len(job_columns))

    width = 0.9
    axes.bar(x, df["est_average_hours"][1:], width, label="With Beaches", color="skyblue")

    axes.set_ylim(0, 45)
    axes.set_xticks(x)
    axes.set_xticklabels(job_columns, rotation=45, ha="right")
    axes.set_ylabel("Average Hours")
    axes.legend()
    plt.tight_layout()


def plot_distance_travelled_by_work_type(connection):
    df = run_query(connection, "SELECT * FROM distance_travelled_by_work_type")

    df['est_average_distance'] = (
                                         df['distance_less_than_5km'] * 2.5 +
                                         df['distance_5_to_10km'] * 7.5 +
                                         df['distance_10_to_30km'] * 20 +
                                         df['distance_30_to_60km'] * 45 +
                                         df['distance_60km_plus'] * 75 +
                                         df['distance_works_from_home'] * 0 +
                                         df['distance_works_offshore_or_abroad'] * 100
                                 ) / (
                                         df['distance_less_than_5km'] +
                                         df['distance_5_to_10km'] +
                                         df['distance_10_to_30km'] +
                                         df['distance_30_to_60km'] +
                                         df['distance_60km_plus'] +
                                         df['distance_works_from_home'] +
                                         df['distance_works_offshore_or_abroad']
                                 )

    job_columns = ['managers_directors_senior_officials', 'professional', 'associate_professional_technical',
                   'administrative_secretarial', 'skilled_trades',
                   'caring_leisure_other_service', 'sales_customer_service', 'process_plant_machine_operatives',
                   'elementary']

    fig, axes = plt.subplots(1, 1, figsize=(24, 8))

    x = np.arange(len(job_columns))

    width = 0.9
    axes.bar(x, df["est_average_distance"][1:], width, label="With Beaches", color="skyblue")

    axes.set_ylim(0, 100)
    axes.set_xticks(x)
    axes.set_xticklabels(job_columns, rotation=45, ha="right")
    axes.set_ylabel("Average Distance")
    axes.legend()
    plt.tight_layout()
