from pyspark.sql import SparkSession
from pyspark.sql.functions import sort_array, collect_list, concat_ws, count
from pyspark.sql.types import IntegerType, StringType, LongType, StructType, StructField
from ilum.api import IlumJob

class SilverToGold(IlumJob):
    def run(self, spark, config):
    # Retrieve the paths to the data files and directories
        animals_silver_path = str(config.get('animals_silver_path'))
        owners_silver_path = str(config.get('owners_silver_path'))
        animals_gold_path = str(config.get('animals_gold_path'))
        owners_gold_path = str(config.get('owners_gold_path'))

    # Read the data from delta files into DataFrames
        animals_gold = spark.read.format('delta').load(animals_silver_path)

        owners_silver = spark.read.format('delta').load(owners_silver_path)

    # Calculate the number of animals owned by each owner.
        animals_count = (
            animals_gold.groupby("owner_id")
            .agg(
                concat_ws(", ", sort_array(collect_list("animal_name"))).alias("animals_names"),
                count("animal_name").alias("animals_qty"),
            )
        )

    # Join the animal count data with the owner data, and add the animal names and quantities
        owners_gold = (
            owners_silver.join(animals_count, animals_count.owner_id == owners_silver.owner_id, "right")
            .select(
                owners_silver.owner_id,
                owners_silver.first_name,
                owners_silver.last_name,
                animals_count.animals_names,
                animals_count.animals_qty,
                owners_silver.mobile,
                owners_silver.email,
            )
            .sort("owner_id")
        )

    # Write the processed data to Delta files in the gold layer
        animals_gold.write.mode('overwrite').format("delta").save(animals_gold_path)

        owners_gold.write.mode('overwrite').format("delta").save(owners_gold_path)

        return "Processing data from silver to gold complete!"