import os
from pyspark.sql import SparkSession
import sys


slice_length = 3
current_slice_number = 0
vector_slice = dict()


def Map(r):
    global vector_slice, current_slice_number, vector_path
    
    if r[1]//slice_length != current_slice_number:
        vector_slice = dict()
        current_vector_path = vector_path + f"_{r[1]//slice_length}"
        read_vector(current_vector_path)
        current_slice_number = r[1]//slice_length

    return [(r[0], r[2]*vector_slice[r[1]])]


def Reduce(r):
    return (r[0], sum(r[1]))


def split_file(path, type="matrix"):
    global matrix_set
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


def remove_old_files(path):
    for f in os.listdir('.'):
        if f.startswith(path + "_"):
            os.remove(f)


def read_vector(path):
    global vector_slice
    vector_file = open(path, "r")
    lines = vector_file.readlines()
    for line in lines:
        j, v = line.split(";")
        vector_slice[int(j)] = float(v)


if __name__ == "__main__":

    global vector_path
    spark = SparkSession.builder.appName("Matrix").getOrCreate()

    remove_old_files(sys.argv[1])
    remove_old_files(sys.argv[2])

    vector_path = sys.argv[2]

    matrix_set = set()

    split_file(sys.argv[1])
    split_file(sys.argv[2], "vector")

    read_vector(vector_path + "_0")

    matrix = spark.read.text(list(matrix_set)).rdd\
        .map(lambda line: tuple(line[0].split(";")))\
        .map(lambda r: (int(r[0]), int(r[1]), float(r[2])))\
        .flatMap(Map)\
        .groupByKey()\
        .map(Reduce)

    print(matrix.collect())

    spark.stop()