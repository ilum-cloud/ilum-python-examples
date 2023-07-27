from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression

from ilum.api import IlumJob


class LogisticRegressionJobExample(IlumJob):

    def run(self, spark_session: SparkSession, config: dict) -> str:
        df = spark_session.read.csv(config.get('inputFilePath', 's3a://ilum-files/Tel-churn.csv'), header=True,
                                    inferSchema=True)

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

        train, test = df.randomSplit([float(config.get('splitX', '0.7')), float(config.get('splitY', '0.3'))],
                                     seed=int(config.get('seed', '42')))

        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=int(config.get('maxIter', '5')))
        lrModel = lr.fit(train)

        predictions = lrModel.transform(test)

        return '{}'.format(predictions.select("customerID", "label", "prediction").limit(
            int(config.get('rowLimit', '5'))).toJSON().collect())
