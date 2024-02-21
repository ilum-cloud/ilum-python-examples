from pyspark.sql import SparkSession
import pandas as pd
from ilum.api import IlumJob

class RawDataToBronze(IlumJob):
    def run(self, spark, config):
    # Retrieve data URLs from the configuration
        animals_url = str(config.get('animals_url'))
        owners_url = str(config.get('owners_url'))
        species_url = str(config.get('species_url'))

    # Retrieve data paths from the configuration
        animals_bronze_path = str(config.get('animals_bronze_path'))
        owners_bronze_path = str(config.get('owners_bronze_path'))
        species_bronze_path = str(config.get('species_bronze_path'))

    # Define data type dictionaries for each dataset
        animals_dtype_dic = {
            "id": int,
            "owner_id": str,
            "specie_id": str,
            "animal_name": str,
            "gender": str,
            "birth_date": str,
            "color": str,
            "weight": str
        }

        owners_dtype_dic = {
            "owner_id": int,
            "first_name": str,
            "last_name": str,
            "mobile": str,
            "email": str
        }

        species_dtype_dic = {
            "specie_id": int,
            "specie_name": str
        }

    # Read data from CSV files into Pandas DataFrames
        pd_animals = pd.read_csv(animals_url, dtype=animals_dtype_dic, keep_default_na=False)
        pd_owners = pd.read_csv(owners_url, dtype=owners_dtype_dic, keep_default_na=False)
        pd_species = pd.read_csv(species_url, dtype=species_dtype_dic, keep_default_na=False)

    # Create corresponding Spark DataFrames from Pandas DataFrames
        spark_animals = spark.createDataFrame(pd_animals)
        spark_owners = spark.createDataFrame(pd_owners)
        spark_species = spark.createDataFrame(pd_species)

    # Sort each Spark DataFrame by the specified column and write to CSV files in the bronze layer
        spark_animals.sort("id").write.mode('overwrite').option("header", True).csv(animals_bronze_path)
        spark_owners.sort("id").write.mode('overwrite').option("header", True).csv(owners_bronze_path)
        spark_species.sort("specie_id").write.mode('overwrite').option("header", True).csv(species_bronze_path)

    # Return a success message
        return "Loading raw data to bronze layer complete!"