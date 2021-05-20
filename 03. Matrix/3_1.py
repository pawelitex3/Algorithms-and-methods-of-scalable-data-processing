from pyspark.sql import SparkSession
import numpy as np
import sys


def Map(r):
    global vector
    return [(r[0], r[2]*vector[r[1]])]


def Reduce(r):
    return (r[0], sum(r[1]))


def read_vector(vector_path):
    global vector
    vector_file = open(vector_path, "r")
    lines = vector_file.readlines()
    for line in lines:
        line = line.split(";")
        vector[int(line[0])] = float(line[1])


if __name__ == "__main__":

    global vector
    spark = SparkSession.builder.appName("Matrix").getOrCreate()

    matrix = spark.read.text(sys.argv[2]).rdd\
        .map(lambda line: tuple(line[0].split(";")))\
        .map(lambda r: (int(r[0]), int(r[1]), float(r[2])))
    vector = np.zeros(int(sys.argv[1]))
    read_vector(sys.argv[3])

    result = matrix.flatMap(Map).groupByKey().map(Reduce)
    
    print(result.collect())

    spark.stop()