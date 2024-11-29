import osmnx as ox
from osmnx._errors import InsufficientResponseError


def amend_df_with_pois_counts(df, tags: dict, distance_km: float = 1.0,
                              lat_column_name="lat",
                              lng_column_name="lng",
                              geometry_column_name="geometry",
                              radius_around_point=False
                              ):
    def count_pois(row):
        try:
            if radius_around_point:
                lat, lng = row[lat_column_name], row[lng_column_name]
                pois = ox.features_from_point((lat, lng), tags, dist=distance_km * 1000)
            else:
                polygon = row[geometry_column_name]
                pois = ox.features_from_polygon(polygon, tags)
            return {tag: len(pois[pois[tag].notnull()]) for tag in tags}

        except InsufficientResponseError:
            # InsufficientResponseError
            return {tag: 0 for tag in tags}

    pois_counts = df.apply(count_pois, axis=1)

    for tag in tags:
        df[f"{tag}_pois_count"] = pois_counts.apply(lambda x: x[tag])

    return df


def get_pois(tags, polygon=None, lat=None, lng=None, radius_around_point=False, distance_km=1):
    try:
        if radius_around_point:
            pois = ox.features_from_point((lat, lng), tags, dist=distance_km * 1000)
        else:
            pois = ox.features_from_polygon(polygon, tags)

        if pois is not None and not pois.empty:
            pois['centroid'] = pois['geometry'].apply(lambda x: x.centroid)
            pois['lat'] = pois['centroid'].apply(lambda x: x.y)
            pois['lng'] = pois['centroid'].apply(lambda x: x.x)
            pois.drop(columns=['centroid'], inplace=True)
        return pois
    except Exception as e:
        # InsufficientResponseError
        return None
