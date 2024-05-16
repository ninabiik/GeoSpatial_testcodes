import logging
from snowflake.spark import SparkSession
from snowflake.connector import connect
import geopandas as gpd
from config import snowflake_config
from snowpark.sql import functions as sf

# Configure logger
logging.basicConfig(filename='Roads LGA.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Roads LGA Join") \
    .config("lagyan ng config if needed") \
    .getOrCreate()

try:
    logger.info("Connecting to Snowflake Database")
    conn = connect(
        user=snowflake_config['sfUser'],
        password=snowflake_config['sfPassword'],
        account=snowflake_config['sfAccount'],
        warehouse=snowflake_config['sfWarehouse'],
        database=snowflake_config['sfDatabase'],
        schema=snowflake_config['sfSchema']
    )
    logger.info("Connected")

    logger.info("Reading data from roads table")
    df_silver_roads = spark.read \
        .format("snowflake") \
        .options(**snowflake_config) \
        .option("dbtable", "SILVER_ROADS_TABLE_NAME") \
        .load()

    logger.info("Reading data from lga table")
    df_silver_lga = spark.read \
        .format("snowflake") \
        .options(**snowflake_config) \
        .option("dbtable", "SILVER_LGA_TABLE_NAME") \
        .load()

    # Perform spatial join using Snowflake function st_intersects
    df_roads_lga_joined = (
        df_silver_roads
        .join(
            df_silver_lga,
            sf.ST_INTERSECTS(
                df_silver_roads["roads_geom_0_REPLACE WITH COLUMN TO INTERSECT"],
                df_silver_lga["lga_geom_0_REPLACE WITH COLUMN TO INTERSECT"]
            ),
            how='LEFT'
        )
    )

    logger.info("Start converting to GeoJSON")
    df_roads_lga_joined_df = df_roads_lga_joined.toPandas()

    # Convert Pandas dataframe to GeoDataFrame
    gdf = gpd.GeoDataFrame(df_roads_lga_joined_df, geometry='geometry')

    # Convert GeoDataFrame to GeoJSON format
    geojson = gdf.to_json()

    # Save GeoJSON to a file
    with open('df_roads_lga_joined_df.geojson', 'w') as f:
        f.write(geojson)

    logger.info("Script execution completed successfully.")

except Exception as e:
    logger.exception("An error occurred: {}".format(str(e)))

finally:
    spark.stop()
