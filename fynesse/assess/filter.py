import geopandas as gpd
from shapely.geometry import Point


def filter_by_proximity(df, lat, lng, radius_km=10):
    center = Point(lat, lng)
    center_projected = gpd.GeoSeries([center], crs="EPSG:4326").to_crs("EPSG:27700").iloc[0]

    df_projected = df.to_crs("EPSG:27700")
    df['distance'] = df_projected.geometry.centroid.distance(
            center_projected
    ) / 1000

    df_filtered = df[df['distance'] <= radius_km].reset_index(drop=True)

    return df_filtered
