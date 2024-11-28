import folium
from branca.colormap import linear


def plot_from_lat_lng(df, lat_column_name="lat", lng_column_name="lng", zoom_start=10,
                      column_names_in_popup=None, color_from_row=None, size_from_row=None
                      ):
    mean_latitude = df[lat_column_name].mean()
    mean_longitude = df[lng_column_name].mean()

    map_center = (mean_latitude, mean_longitude)
    m = folium.Map(location=map_center, zoom_start=zoom_start)

    def create_circle_for_row(row):
        if column_names_in_popup:
            popup_content = "<div style='font-size: 16px;'>"
            popup_content += "<table>"
            for col in column_names_in_popup:
                popup_content += (
                        f"<tr>"
                        f"<th style='padding: 6px; font-weight: bold;'>{str(col).title()}</th>"
                        f"<td style='padding: 6px; color: #555;'>{str(row[col]).title()}</td>"
                        f"</tr>"
                )
            popup_content += "</table></div>"
        else:
            popup_content = None

        folium.CircleMarker(
                location=[row[lat_column_name], row[lng_column_name]],
                radius=10 if size_from_row is None else size_from_row(row),
                color=color_from_row(row) if color_from_row else 'blue',
                fill=True,
                popup=folium.Popup(popup_content, max_width=300) if popup_content else None
        ).add_to(m)

    df.apply(create_circle_for_row, axis=1)

    return m


def plot_from_geometry(df, geometry_column_name="Geometry", lat_column_name="lat",
                       lng_column_name="lng", zoom_start=10, column_names_in_popup=None,
                       color_from_row=None, size_from_row=None
                       ):
    mean_latitude = df[lat_column_name].mean()
    mean_longitude = df[lng_column_name].mean()

    map_center = (mean_latitude, mean_longitude)
    m = folium.Map(location=map_center, zoom_start=zoom_start)

    def create_geojson_for_row(row):
        if column_names_in_popup:
            popup_content = "<div style='font-size: 16px;'>"
            popup_content += "<table>"
            for col in column_names_in_popup:
                popup_content += (
                        f"<tr>"
                        f"<th style='padding: 6px; font-weight: bold;'>{str(col).title()}</th>"
                        f"<td style='padding: 6px; color: #555;'>{str(round(row[col], 3) if isinstance(row[col], (int, float)) else row[col]).title()}</td>"
                        f"</tr>"
                )
            popup_content += "</table></div>"
        else:
            popup_content = None

        geo_style = {
                'color'      : color_from_row(row) if color_from_row else 'blue',
                'weight'     : 2,
                'fillColor'  : color_from_row(row) if color_from_row else 'blue',
                'fillOpacity': 0.4,
        }

        folium.GeoJson(
                row[geometry_column_name].__geo_interface__,
                style_function=lambda x: geo_style,
                popup=folium.Popup(popup_content, max_width=300) if popup_content else None
        ).add_to(m)

    df.apply(create_geojson_for_row, axis=1)

    return m


def plot_with_colormap(df,
                       color_map_column,
                       geometry_column_name="geometry",
                       lat_column_name="lat",
                       lng_column_name="lng",
                       zoom_start=10,
                       column_names_in_popup=None,
                       size_from_row=None,
                       from_lat_lng=False,
                       ):
    min_val = df[color_map_column].min()
    max_val = df[color_map_column].max()

    colormap = linear.viridis.scale(min_val, max_val)

    m = plot_from_geometry(df,
                           geometry_column_name=geometry_column_name,
                           lat_column_name=lat_column_name,
                           lng_column_name=lng_column_name,
                           column_names_in_popup=column_names_in_popup,
                           zoom_start=zoom_start,
                           color_from_row=lambda row: colormap(row[color_map_column]),
                           size_from_row=size_from_row
                           )

    if from_lat_lng:
        m = plot_from_lat_lng(df,
                              column_names_in_popup=column_names_in_popup,
                              zoom_start=zoom_start,
                              color_from_row=lambda row: colormap(row[color_map_column])
                              )

    colormap.caption = color_map_column
    colormap.add_to(m)
    return m
