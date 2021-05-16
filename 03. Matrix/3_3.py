from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors
import numpy as np
import os
import sys

strip_size = 2


def Map_1(r):
    if r[0] == 'A':
        return [(r[2], (r[0], r[1], r[3]))]
    elif r[0] == 'B':
        return [(r[1], (r[0], r[2], r[3]))]
    return []


def Map_2(r):
    return r


def Reduce_1(r):
    A = list(filter(lambda x: x[0] == 'A', r[1]))
    B = list(filter(lambda x: x[0] == 'B', r[1]))
    return [((a[1], b[1]), a[2]*b[2]) for a in A for b in B]


def Reduce_2(r):
    return [(r[0], sum(r[1]))]


if __name__ == "__main__":

    global matrix, vector, spark
    spark = SparkSession.builder.appName("Matrix").getOrCreate()

    matrix_A = spark.read.text("Matrix_A").rdd\
        .map(lambda line: tuple(line[0].split(";")))\
        .map(lambda r: (r[0], int(r[1]), int(r[2]), float(r[3])))\
        .flatMap(Map_1)

    matrix_B = spark.read.text("Matrix_B").rdd\
        .map(lambda line: tuple(line[0].split(";")))\
        .map(lambda r: (r[0], int(r[1]), int(r[2]), float(r[3])))\
        .flatMap(Map_1)

    matrix_C = matrix_A.union(matrix_B).groupByKey().flatMap(Reduce_1).groupByKey().flatMap(Reduce_2)
    
    print(matrix_C.collect())
    #print(matrix_B.collect())

    spark.stop()