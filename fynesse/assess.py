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
