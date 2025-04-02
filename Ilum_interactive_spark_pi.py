from random import random
from operator import add

from ilum.api import IlumJob


class SparkPiInteractiveExample(IlumJob):

    def run(self, spark, config):
        partitions = int(config.get('partitions', '5'))
        n = 100000 * partitions

        def is_inside_unit_circle(_: int) -> float:
            x = random() * 2 - 1
            y = random() * 2 - 1
            return 1.0 if x ** 2 + y ** 2 <= 1 else 0.0

        count = (
            spark.sparkContext.parallelize(range(1, n + 1), partitions)
            .map(is_inside_unit_circle)
            .reduce(add)
        )

        pi_approx = 4.0 * count / n
        return f"Pi is roughly {pi_approx}"