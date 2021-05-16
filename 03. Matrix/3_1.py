from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors
import numpy as np
import os
import sys

strip_size = 2

def Map(r):
    global vector
    return [(r[0], r[2]*vector[r[1]])]


def Reduce(r):
    return (r[0], sum(r[1]))


def read_vector():
    global vector
    vector_file = open("v1", "r")
    lines = vector_file.readlines()
    for line in lines:
        line = line.split(";")
        vector[int(line[0])] = float(line[1])


if __name__ == "__main__":

    global matrix, vector, spark, n
    spark = SparkSession.builder.appName("Matrix").getOrCreate()

    matrix = spark.read.text("M1").rdd.map(lambda line: tuple(line[0].split(";"))).map(lambda r: (int(r[0]), int(r[1]), float(r[2])))
    n = int(sys.argv[1])
    vector = np.zeros(n)
    read_vector()

    result = matrix.flatMap(Map).groupByKey().map(Reduce)
    print(result.collect())

    spark.stop()