from random import random
from operator import add

from ilum.api import IlumJob


class SparkPiInteractiveExample(IlumJob):

    def run(self, spark, config):
        partitions = int(config.get('partitions', '5'))
        n = 100000 * partitions

        def f(_: int) -> float:
            x = random() * 2 - 1
            y = random() * 2 - 1
            return 1 if x ** 2 + y ** 2 <= 1 else 0

        count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)

        return "Pi is roughly %f" % (4.0 * count / n)
