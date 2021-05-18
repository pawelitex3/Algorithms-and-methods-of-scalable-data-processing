from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors
import numpy as np
import os
import math
import sys

slice_length = 3

def Map(r):
    global spark, vector_path
    current_vector_path = vector_path + f"_{r[1]//slice_length}"
    vector_file = open(current_vector_path, "r")
    lines = vector_file.readlines()
    for line in lines:
        j, v = line.split(";")
        if int(j) == r[1]:
            return [(r[0], float(v)*r[2])]
    return []
    
    vector_file.close()
    #print(vector.collect())

    return [r]


def Reduce(r):
    return (r[0], sum(r[1]))


# def read_vector():
#     global vector
#     vector_file = open("v1", "r")
#     lines = vector_file.readlines()
#     for line in lines:
#         line = line.split(";")
#         vector[int(line[0])] = float(line[1])

def split_file(path, type="matrix"):
    global matrix_set, vector_set
    current_file = open(path, "r")
    lines = current_file.readlines()
    for line in lines:
        values = line.split(";")
        if type == "matrix":
            new_path = path + f"_{int(values[1])//slice_length}"
            with open(new_path, "a") as output:
                output.write(line)
            matrix_set.add(new_path)
        elif type == "vector":
            new_path = path + f"_{int(values[0])//slice_length}"
            with open(new_path, "a") as output:
                output.write(line)
            vector_set.add(new_path)



if __name__ == "__main__":

    global matrix, vector, spark, n, matrix_set, vector_set, vector_path
    spark = SparkSession.builder.appName("Matrix").getOrCreate()

    matrix_set = set()
    vector_set = set()

    vector_path = "v1"

    split_file("M1")
    split_file("v1", "vector")

    print(matrix_set)

    matrix = spark.read.text(list(matrix_set)).rdd.map(lambda line: tuple(line[0].split(";"))).map(lambda r: (int(r[0]), int(r[1]), float(r[2]))).flatMap(Map).groupByKey().map(Reduce)

    # result = matrix.flatMap(Map).groupByKey().map(Reduce)
    print(matrix.collect())

    spark.stop()