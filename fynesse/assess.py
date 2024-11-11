import math

import branca
import folium
import pandas as pd
import seaborn as sns
from haversine import haversine, Unit
from matplotlib import pyplot as plt
from scipy.spatial.distance import cdist
from sklearn.preprocessing import StandardScaler

from . import access

DEGREE_KM_RATIO = 111


def data():
    """Load the data from access and ensure missing values are correctly encoded as well as indices correct, column names informative, date and times correctly formatted. Return a structured data structure such as a data frame."""
    df = access.data()
    raise NotImplementedError


def query(data):
    """Request user input for some aspect of the data."""
    raise NotImplementedError


def view(data):
    """Provide a view of the data that allows the user to verify some aspect of its quality."""
    raise NotImplementedError


def labelled(data):
    """Provide a labelled set of data ready for supervised learning."""
    raise NotImplementedError


def get_pois_near_coordinates(latitude: float, longitude: float, tags: dict,
                              distance_km: float = 1.0
                              ):
    box_length_in_degrees = distance_km / DEGREE_KM_RATIO
    north = latitude + box_length_in_degrees / 2
    south = latitude - box_length_in_degrees / 2
    west = longitude - box_length_in_degrees / 2
    east = longitude + box_length_in_degrees / 2

    out = pd.DataFrame(ox.features_from_bbox(bbox=(north, south, east, west), tags=tags))
    out['latitude'] = out.apply(lambda row: row.geometry.centroid.y, axis=1)
    out['longitude'] = out.apply(lambda row: row.geometry.centroid.x, axis=1)
    return out


# This returns a df of neraby pois by tag and count
def count_pois_near_coordinates(latitude: float, longitude: float, tags: dict,
                                distance_km: float = 1.0
                                ):
    features = get_pois_near_coordinates(latitude, longitude, tags, distance_km)

    feature_count_by_tag = {}
    for tag in tags:
        if tag in features:
            feature_count_by_tag[tag] = features[tag].notnull().sum()
        else:
            feature_count_by_tag[tag] = 0

    feature_count_by_tag_df = pd.Series(feature_count_by_tag)

    return feature_count_by_tag_df


def count_pois_by_location(locations_dict: dict, tags: dict, distance_km: float = 6.0):
    count_pois_dfs = []
    for location, (latitude, longitude) in locations_dict.items():
        count_pois_df = count_pois_near_coordinates(latitude, longitude, tags, distance_km
                                                    ).to_frame().T
        count_pois_df['location'] = location
        count_pois_dfs.append(count_pois_df)

    count_pois_by_location_df = pd.concat(count_pois_dfs, ignore_index=True)
    count_pois_by_location_df.set_index('location', inplace=True)
    return count_pois_by_location_df


def plot_locations(locations_dict, zoom_start=12):
    latitudes = [lat for (lat, lng) in locations_dict.values()]
    longitudes = [lng for (lat, lng) in locations_dict.values()]

    mean_latitude = sum(latitudes) / len(locations_dict)
    mean_longitude = sum(longitudes) / len(locations_dict)

    map_center = (mean_latitude, mean_longitude)
    m = folium.Map(location=map_center, zoom_start=zoom_start)

    for location, (latitude, longitude) in locations_dict.items():
        folium.CircleMarker(
                location=[latitude, longitude],
                radius=10,
                color="red",
                fill=True,
        ).add_to(m)

    return m


def kmeans_clustering(df, n_clusters=3):
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    numeric_df = df.select_dtypes(include=['float64', 'int64'])

    scaler = StandardScaler()
    scaled = scaler.fit_transform(numeric_df)
    kmeans = KMeans(n_clusters=n_clusters)
    df['cluster'] = kmeans.fit_predict(scaled)
    cluster_centers = scaler.inverse_transform(kmeans.cluster_centers_)
    return cluster_centers


def normalise(df):
    scaler = StandardScaler()
    scaled = scaler.fit_transform(df)
    return pd.DataFrame(scaled, columns=df.columns, index=df.index)


def compute_correlation_matrix(normalised_df):
    return normalised_df.corr()


def plot_correlation_matrix(correlation_matrix):
    plt.figure(figsize=(12, 10))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', cbar=True, square=True,
                linewidths=0.5, annot_kws={'size': 10}
                )
    plt.show()


def compute_distance_matrix(normalised_df):
    data = normalised_df.values
    distance_matrix = cdist(data, data, metric='euclidean')
    distance_matrix_df = pd.DataFrame(distance_matrix, index=normalised_df.index,
                                      columns=normalised_df.index
                                      )
    return distance_matrix_df


def plot_distance_matrix(distance_matrix_df):
    plt.figure(figsize=(10, 8))

    sns.heatmap(distance_matrix_df, cmap='viridis', cbar=True, square=True, annot=False,
                xticklabels=True, yticklabels=True
                )

    plt.show()


def select_houses_in_box(connection, latitude, longitude, region_distance_km):
    half_region_distance_km = region_distance_km / 2
    max_lat = latitude + half_region_distance_km / DEGREE_KM_RATIO
    min_lat = latitude - half_region_distance_km / DEGREE_KM_RATIO
    max_lng = longitude + half_region_distance_km / DEGREE_KM_RATIO
    min_lng = longitude - half_region_distance_km / DEGREE_KM_RATIO

    query = f"""
    SELECT * FROM prices_coordinates_data 
      WHERE latitude BETWEEN {min_lat} AND {max_lat} 
        AND longitude BETWEEN {min_lng} AND {max_lng} 
        AND transaction_date > '2020-01-01'
  """

    return access.run_query(connection, query)


def select_house_in_radius(connection, latitude, longitude, region_distance_km):
    houses_in_box = select_houses_in_box(connection, latitude, longitude, region_distance_km)
    houses_in_radius = houses_in_box[
        houses_in_box.apply(
                lambda row: haversine((row['latitude'], row['longitude']), (latitude, longitude),
                                      unit=Unit.KILOMETERS
                                      ) < 1, axis=1
        )
    ]
    return houses_in_radius


def plot_locations_from_df(df, zoom_start=10):
    mean_latitude = df["latitude"].mean()
    mean_longitude = df["longitude"].mean()

    map_center = (mean_latitude, mean_longitude)
    m = folium.Map(location=map_center, zoom_start=zoom_start)

    def create_circle_for_row(row):
        if row["addr:housenumber"] is None or row["addr:street"] is None or row[
            "addr:postcode"] is None:
            color = "red"
        else:
            color = "grey"
        folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=10,
                color=color,
                fill=True
        ).add_to(m)

    df.apply(create_circle_for_row, axis=1)

    return m


def match_houses(pp_data, osm_data):
    pp_data = pp_data.rename(columns={
            "postcode": "postcode"
    }
    )

    osm_data = osm_data.rename(columns={
            "addr:housenumber": "housenumber",
            "addr:postcode"   : "postcode"
    }
    )

    pp_data["housenumber"] = pp_data["primary_addressable_object_name"].str.extract(r'(\d+)'
                                                                                    ).fillna('')

    pp_data["housenumber"] = pp_data["housenumber"].str.lower()
    pp_data["postcode"] = pp_data["postcode"].str.lower()
    osm_data["housenumber"] = osm_data["housenumber"].astype(str).str.lower()
    osm_data["postcode"] = osm_data["postcode"].astype(str).str.lower()

    matched_houses = pd.merge(
            pp_data,
            osm_data,
            on=["housenumber", "postcode"],
            how="inner",
            suffixes=('_pp', '_osm')
    )

    return matched_houses


def expand_osm_house_ranges(osm_data):
    expanded_rows = []

    for _, row in osm_data.iterrows():
        housenumber = str(row['addr:housenumber'])
        if '-' in housenumber:
            try:
                start, end = map(int, housenumber.split('-'))
                if end - start > 100:
                    raise ValueError()
                for number in range(start, end + 1):
                    new_row = row.copy()
                    new_row['addr:housenumber'] = str(number)
                    row['osm_house_range'] = housenumber
                    expanded_rows.append(new_row)
            except ValueError:
                row['osm_house_range'] = housenumber
                expanded_rows.append(row)
        else:
            row['osm_house_range'] = housenumber
            expanded_rows.append(row)
        expanded_osm_data = pd.DataFrame(expanded_rows)

    return expanded_osm_data


def select_postcodes_in_box(connection, latitude, longitude, region_distance_km):
    half_region_distance_km = region_distance_km / 2
    max_lat = latitude + half_region_distance_km / DEGREE_KM_RATIO
    min_lat = latitude - half_region_distance_km / DEGREE_KM_RATIO
    max_lng = longitude + half_region_distance_km / DEGREE_KM_RATIO
    min_lng = longitude - half_region_distance_km / DEGREE_KM_RATIO

    query = f"""
    SELECT postcode FROM postcode_data
      WHERE latitude BETWEEN {min_lat} AND {max_lat}
        AND longitude BETWEEN {min_lng} AND {max_lng}
  """

    return access.run_query(connection, query)


def pp_houses_from_postcode(connection, postcodes):
    dfs = []
    for i, postcode in enumerate(postcodes["postcode"]):
        query = f"""
          SELECT * FROM pp_data
            WHERE postcode = '{postcode}'
        """
        dfs.append(access.run_query(connection, query))
    return pd.concat(dfs, ignore_index=True)


def plot_prices(houses_df, price_col="price", log=True):
    map_center = [houses_df['latitude'].mean(), houses_df['longitude'].mean()]
    folium_map = folium.Map(location=map_center, zoom_start=12)

    max_price = math.log(houses_df[price_col].max()) if log else houses_df[price_col].max()
    min_price = math.log(houses_df[price_col].min()) if log else houses_df[price_col].min()
    colormap = branca.colormap.LinearColormap(
            colors=['blue', 'red'],
            vmin=min_price, vmax=max_price
    ).to_step(n=10)

    for _, row in houses_df.iterrows():
        price = row[price_col]
        latitude = row['latitude']
        longitude = row['longitude']

        color = colormap(math.log(price) if log else price)

        folium.CircleMarker(
                location=(latitude, longitude),
                radius=7,
                color=color,
                fill=True,
                fill_opacity=0.9,
                popup=f"""
            {"Price" if price_col == "price" else price_col}: ${price:,.2f}\n\n
            Name:{row['primary_addressable_object_name']}\n\n
            Date Sold:{row['date_of_transfer']}\n\n
            {row['housenumber']} {row['addr:street']} {row['postcode']}
            """,
        ).add_to(folium_map)

    folium_map.add_child(colormap)

    return folium_map


def adjust_for_inflation(houses_df):
    houses_df['date_of_transfer_date'] = pd.to_datetime(houses_df['date_of_transfer'])
    houses_df['year'] = houses_df['date_of_transfer_date'].dt.year
    cpi_df = pd.read_csv(
            "https://raw.githubusercontent.com/FlamingoWinter/ads_practicals/refs/heads/main/cpi.csv"
    )
    cpi_df.rename(columns={'Title': 'year'}, inplace=True)
    houses_df = houses_df.merge(cpi_df, on='year', how='left')
    houses_df["price_adjusted"] = houses_df["price"] / houses_df[
        "CPI"] * 128.6  # current CPI (or 2023's because that's the last one ONS had data for)
    return houses_df


def get_matched_houses(connection, latitude, longitude, region_distance_km=2.0):
    buildings = get_pois_near_coordinates(latitude, longitude, {"building": True},
                                          region_distance_km
                                          )
    expanded_buildings = expand_osm_house_ranges(buildings)

    postcodes = select_postcodes_in_box(connection, latitude, longitude, region_distance_km)
    houses = pp_houses_from_postcode(connection, postcodes)

    return match_houses(houses, expanded_buildings)
