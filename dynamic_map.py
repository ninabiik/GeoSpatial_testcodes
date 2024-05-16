import streamlit as st
import pydeck as pdk
from math import floor
from snowflake.snowpark.context import get_active_session
import altair as alt
import time


def get_color(row):
    color_scheme = [
        [197, 224, 180],  # Light green
        [255, 255, 191],  # Light yellow
        [254, 224, 144],  # Orange
        [252, 141, 89],  # Yellow-orange
        [215, 48, 39]  # Red
    ]
    min_count = df['TRAFFIC_COUNT'].min()
    max_count = df['TRAFFIC_COUNT'].max()
    diff = max_count - min_count
    number_of_colors = len(color_scheme)
    index = floor(number_of_colors * (row['TRAFFIC_COUNT'] - min_count) / diff)
    # the index might barely go out of bounds, so correct for that:
    if index == number_of_colors:
        index = number_of_colors - 1
    elif index == -1:
        index = 0
    return color_scheme[index]


def generateLayer(filtered_df, max_range):
    return pdk.Layer(
        "H3HexagonLayer",
        filtered_df,
        pickable=True,
        stroked=True,
        filled=True,
        extruded=True,
        get_hexagon="HEX",
        get_fill_color="COLOR_COLUMN",
        get_line_color=[255, 255, 255],
        get_elevation="TRAFFIC_COUNT",
        elevation_scale=0.1,
        elevation_range=[0, max_range],
        line_width_min_pixels=2
    )


def getData():
    # Snowflake query
    session = get_active_session()
    snowpark_df = session.sql("""
    SELECT ROAD_NAME,SUBURB,YEAR,PERIOD,SUM(TRAFFIC_COUNT) as TRAFFIC_COUNT,WGS84_LATITUDE,WGS84_LONGITUDE,HEX FROM (
    select ROAD_NAME, SUBURB, CLASSIFICATION_TYPE, YEAR, PERIOD, CAST(TRAFFIC_COUNT AS INTEGER) AS TRAFFIC_COUNT, CAST(WGS84_LATITUDE AS FLOAT) as WGS84_LATITUDE, CAST(WGS84_LONGITUDE AS FLOAT) as WGS84_LONGITUDE,H3_POINT_TO_CELL_STRING(TO_GEOGRAPHY(WKT_GEOM),7) as HEX FROM TEST_DB.BENCHJAM_GEOSPATIAL.RAW_TRAFFIC_VOLUME
    )
    GROUP BY 1,2,3,4,6,7,8;
    """)
    df = snowpark_df.toPandas()
    return df


def create_bar_chart(filtered_df):
    color_scheme = [
        [197, 224, 180],  # Light green
        [255, 255, 191],  # Light yellow
        [254, 224, 144],  # Orange
        [252, 141, 89],  # Yellow-orange
        [215, 48, 39]  # Red
    ]
    top_streets = filtered_df.groupby(['YEAR', 'PERIOD', 'ROAD_NAME', 'SUBURB']).agg({'TRAFFIC_COUNT': 'sum'}).nlargest(
        5, 'TRAFFIC_COUNT').reset_index()

    # Sort the data by traffic count in descending order
    top_streets = top_streets.sort_values(by='TRAFFIC_COUNT', ascending=True)

    # Define custom color scale using the provided color scheme
    color_scale = alt.Scale(
        domain=top_streets['ROAD_NAME'].tolist(),  # Use road names as domain for color scale
        range=[f'rgb({r},{g},{b})' for r, g, b in color_scheme]
    )

    chart = alt.Chart(top_streets).mark_bar().encode(
        x=alt.X('TRAFFIC_COUNT', title='Traffic Count'),
        y=alt.Y('ROAD_NAME', sort='-x', title='Road Name'),
        color=alt.Color('ROAD_NAME', scale=color_scale, legend=None),  # Use custom color scale
        tooltip=['YEAR', 'PERIOD', 'ROAD_NAME', 'SUBURB', 'TRAFFIC_COUNT']
    ).properties(
        title=''
    )
    return chart


# Streamlit App
def main(df):
    # Set Refresh Interval
    global filtered_df

    REFRESH_INTERVAL = 10

    # Get list of year
    list_of_year = df['YEAR'].unique()
    filtered_df = df.dropna(subset=['YEAR'])

    view_placeholder = st.empty()
    map_view = pdk.ViewState(
        latitude=df['WGS84_LATITUDE'].mean(),
        longitude=df['WGS84_LONGITUDE'].mean(),
        zoom=10,
        pitch=45,
    )
    df['COLOR_COLUMN'] = df.apply(get_color, axis=1)

    # Display map using Pydeck HexagonLayer
    while True:
        for year in list_of_year:
            filtered_df = df[df['YEAR'] == year]
            max_range = filtered_df["TRAFFIC_COUNT"].max()
            map_layer = generateLayer(filtered_df, max_range)

            view_timer = REFRESH_INTERVAL + 1
            while view_timer > 0:
                time.sleep(1)  # Wait for 1 second
                view_timer -= 1

                with view_placeholder.container():
                    col1, cols_space, col2 = st.columns([4, 0.5, 2])
                    with col1:
                        st.title('Sydney Traffic Volume Trends: A Time Series Analysis, 2006-2024')
                        st.text(f'Refreshing Data in {view_timer} seconds...')
                        st.text(f'Exploring Spatial Traffic Patterns Across Sydney')
                        st.pydeck_chart(
                            pdk.Deck(
                                map_style='mapbox://styles/mapbox/navigation-preview-night-v4',
                                initial_view_state=map_view,
                                tooltip={
                                    "text": "Road name: {ROAD_NAME}\n Suburb: {SUBURB}\n Traffic Count: {TRAFFIC_COUNT}\n Year: {YEAR}\n Period: {PERIOD}"},
                                layers=[map_layer],
                            )
                        )
                        st.subheader("Legend")
                        st.markdown("""
                        - Hexagon Elevation: Visualises total traffic count for the chosen time frame
                        """)
                        st.write(filtered_df)
                    with col2:
                        st.subheader("Annual High Traffic Leaders: Sydney's Busiest Roads")
                        st.write(create_bar_chart(filtered_df))


if __name__ == "__main__":
    df = getData()
    main(df)