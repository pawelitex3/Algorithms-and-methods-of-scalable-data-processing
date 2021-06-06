from pyspark.sql import SparkSession
import numpy as np
import sys


def Map(r):
    global vector
    return [(r[0], r[2]*vector[r[1]])]


def Reduce(r):
    return (r[0], sum(r[1]))


def fraction_to_float(fraction):
    try:
        return float(fraction)
    except:
        numerator, denominator = fraction.split("/")
        return float(float(numerator)/float(denominator))


if __name__ == "__main__":

    global vector
    spark = SparkSession.builder.appName("PageRank").getOrCreate()

    matrix = spark.read.text(sys.argv[1]).rdd\
        .map(lambda line: tuple(line[0].split(";")))\
        .map(lambda r: (int(r[0]), int(r[1]), fraction_to_float(r[2])))

    n = int(sys.argv[2])
    vector = np.full(n, 1/n, dtype=float)

    print(vector)

    for i in range(50):
        new_vector = matrix.flatMap(Map).groupByKey().map(Reduce)
        vector = new_vector.collectAsMap()
        print(vector)

    spark.stop()