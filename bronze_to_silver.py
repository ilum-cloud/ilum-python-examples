from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType, StringType, LongType, StructType, StructField
from ilum.api import IlumJob


class BronzeToSilver(IlumJob):
    def run(self, spark, config):
    # Retrieve the paths to the data files and directories
        animals_bronze_path = str(config.get('animals_bronze_path'))
        owners_bronze_path = str(config.get('owners_bronze_path'))
        species_bronze_path = str(config.get('species_bronze_path'))
        animals_silver_path = str(config.get('animals_silver_path'))
        owners_silver_path = str(config.get('owners_silver_path'))

    # Define the schema for the animals and owners data frames
        animals_bronze_schema = StructType([
            StructField('id', IntegerType(), False),
            StructField('owner_id', IntegerType(), False),
            StructField('specie_id', IntegerType(), False),
            StructField('animal_name', StringType(), False),
            StructField('gender', StringType(), False),
            StructField('birth_date', StringType(), False),
            StructField('color', StringType(), False),
            StructField('size', StringType(), False),
            StructField('weight', StringType(), False)
        ])

        owners_bronze_schema = StructType([
            StructField('owner_id', IntegerType(), False),
            StructField('first_name', StringType(), False),
            StructField('last_name', StringType(), False),
            StructField('mobile', LongType(), False),
            StructField('email', StringType(), False)
        ])

        species_bronze_schema = StructType([
			StructField("specie_id", IntegerType(), False),
			StructField("specie_name", StringType(), False)
		])

    # Read the data from the CSV files into DataFrames
        animals_bronze = spark.read.csv(
            path=animals_bronze_path,
            header=True,
            schema=animals_bronze_schema
        ).dropna()

        owners_silver = spark.read.csv(
            path=owners_bronze_path,
            header=True,
            schema=owners_bronze_schema
        ).dropna()

        species_bronze = spark.read.csv(
            path=species_bronze_path,
            header=True,
            schema=species_bronze_schema
        ).dropna()

    # Join the animals and species DataFrames to link each animal to its corresponding species
        animals_silver = animals_bronze.join(
            species_bronze, animals_bronze['specie_id'] == species_bronze['specie_id'], 'left'
        ).select(
            animals_bronze['id'],
            animals_bronze['owner_id'],
            species_bronze['specie_name'],
            animals_bronze['animal_name'],
            to_date(animals_bronze['birth_date'], 'MM/dd/yyyy').alias('birth_date'),
            animals_bronze['gender'],
            animals_bronze['size'],
            animals_bronze['color'],
            animals_bronze['weight'],
        )

    # Write the processed data to Delta files in the silver layer
        animals_silver.write.mode('overwrite').format("delta").save(animals_silver_path)

        owners_silver.write.mode('overwrite').format("delta").save(owners_silver_path)

        return "Processing data from bronze to silver complete!"
