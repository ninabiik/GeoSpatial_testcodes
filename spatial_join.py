import logging
from snowflake.spark import SparkSession
from snowflake.connector import connect
import geopandas as gpd
from config import snowflake_config

# Configure logger
logging.basicConfig(filename='Spatial Join.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Traffic Light Matching") \
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

    logger.info("Reading data from traffic lights table")
    traffic_lights_spark_df = spark.read \
        .format("snowflake") \
        .options(**snowflake_config) \
        .option("dbtable", "TRAFFIC_LIGHTS_TABLE_NAME") \
        .load()

    logger.info("Reading data from Road LGA table")
    road_lga_spark_df = spark.read \
        .format("snowflake") \
        .options(**snowflake_config) \
        .option("dbtable", "ROAD_LGA_TABLE_NAME") \
        .load()

    logger.info("Creating temporary views")
    traffic_lights_spark_df.createOrReplaceTempView("traffic_lights")
    road_lga_spark_df.createOrReplaceTempView("road_lga")

    logger.info("Running Spark Query")
    matched_traffic_lights = spark.sql("""
        SELECT tl.*, rl.*
        FROM traffic_lights tl
        LEFT JOIN road_lga rl
        ON ST_INTERSECTS(ST_BUFFER(tl.geometry, 0.75), rl.geometry)
    """)

    matched_traffic_lights.show()

    logger.info("Start converting to GeoJSON")
    matched_traffic_lights_pandas_df = matched_traffic_lights.toPandas()

    # Convert Pandas dataframe to GeoDataFrame
    gdf = gpd.GeoDataFrame(matched_traffic_lights_pandas_df, geometry='geometry')

    # Convert GeoDataFrame to GeoJSON format
    geojson = gdf.to_json()

    # Save GeoJSON to a file
    with open('matched_traffic_lights.geojson', 'w') as f:
        f.write(geojson)

    logger.info("Script execution completed successfully.")

except Exception as e:
    logger.exception("An error occurred: {}".format(str(e)))

finally:
    spark.stop()
