from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("IlumAdvancedPythonExample") \
        .getOrCreate()

    df = spark.read.csv('s3a://ilum-files/Tel-churn.csv', header=True, inferSchema=True)

    categoricalColumns = ['gender', 'Partner', 'Dependents', 'PhoneService', 'MultipleLines', 'InternetService',
                          'OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV',
                          'StreamingMovies', 'Contract', 'PaperlessBilling', 'PaymentMethod']

    stages = []

    for categoricalCol in categoricalColumns:
        stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
        stages += [stringIndexer]

    label_stringIdx = StringIndexer(inputCol="Churn", outputCol="label")
    stages += [label_stringIdx]

    numericCols = ['SeniorCitizen', 'tenure', 'MonthlyCharges']

    assemblerInputs = [c + "Index" for c in categoricalColumns] + numericCols
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
    stages += [assembler]

    pipeline = Pipeline(stages=stages)
    pipelineModel = pipeline.fit(df)
    df = pipelineModel.transform(df)

    train, test = df.randomSplit([0.7, 0.3], seed=42)

    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
    lrModel = lr.fit(train)

    predictions = lrModel.transform(test)

    predictions.select("customerID", "label", "prediction").show(5)
    predictions.select("customerID", "label", "prediction").write.option("header", "true") \
        .csv('s3a://ilum-files/predictions')

    spark.stop()
