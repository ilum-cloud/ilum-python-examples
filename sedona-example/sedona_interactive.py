from sedona.spark import *
import geopandas as gpd
import s3fs
from ilum.api import IlumJob

class MatchAirportsWithCountries(IlumJob):
    def run(self, spark, config):
    # Retrieve data path from the configuration
        countries_path = str(config.get('countries_path'))
        airports_path = str(config.get('airports_path'))
        result_path = str(config.get('result_path'))

    # Retrive country name from the config
        country = str(config.get('country'))

    # Retrive save parameter from the config
        save = config.get('save').lower() == 'true'

    # Initialize SedonaContext
        SedonaContext.create(spark)

    # Configure s3fs to accessiong the data
        s3 = s3fs.S3FileSystem(
          key='minioadmin',
          secret='minioadmin',
          endpoint_url='http://ilum-minio:9000/'
       )

    # Load and process country data
        countries_gpd = gpd.read_file(s3.open(countries_path), engine='pyogrio')
        countries_df = spark.createDataFrame(countries_gpd)
        countries_df.createOrReplaceTempView("country")

    # Load and process airport data
        airports_gpd = gpd.read_file(s3.open(airports_path), engine='pyogrio')
        airports_df = spark.createDataFrame(airports_gpd)
        airports_df.createOrReplaceTempView("airport")

    # Match airports with countries based on their location using SQL API
        result = spark.sql("""
            SELECT 
                c.geometry as country_geom, 
                c.NAME_EN, 
                a.geometry as airport_geom, 
                a.name 
            FROM 
                country c, 
                airport a 
            WHERE 
                ST_Contains(c.geometry, a.geometry)
        """)
        result.createOrReplaceTempView("result")

    # Group airports based on the country they belong to and filter expected country
        singleresult = spark.sql(f"SELECT c.NAME_EN, c.country_geom, count(*) as airports_count FROM result c WHERE c.NAME_EN = '{country}' GROUP BY c.NAME_EN, c.country_geom")

    # Upload the processed data to database
        if save:  # Assuming save is a boolean variable (True or False)
          save_path = f"{result_path}/{country}.shp"
          singleresult.write.mode('overwrite').format("geoparquet").save(save_path)

    # Extract the num_airports and return it
        num_airports = singleresult.first().airports_count
        return f"Data processing finished successfully.\nIn {country} there are {num_airports} airports."
