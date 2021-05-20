from pyspark.sql import SparkSession
import sys

def Map_1(r):
    if r[0] == 'A':
        return [(r[2], (r[0], r[1], r[3]))]
    elif r[0] == 'B':
        return [(r[1], (r[0], r[2], r[3]))]
    return []


def Map_2(r):
    return [r]


def Reduce_1(r):
    A = list(filter(lambda x: x[0] == 'A', r[1]))
    B = list(filter(lambda x: x[0] == 'B', r[1]))
    return [((a[1], b[1]), a[2]*b[2]) for a in A for b in B]


def Reduce_2(r):
    return [(r[0], sum(r[1]))]


if __name__ == "__main__":

    spark = SparkSession.builder.appName("Matrix").getOrCreate()

    matrix_A = spark.read.text(sys.argv[1]).rdd\
        .map(lambda line: tuple(line[0].split(";")))\
        .map(lambda r: (r[0], int(r[1]), int(r[2]), float(r[3])))

    matrix_B = spark.read.text(sys.argv[2]).rdd\
        .map(lambda line: tuple(line[0].split(";")))\
        .map(lambda r: (r[0], int(r[1]), int(r[2]), float(r[3])))

    matrix_C = matrix_A.union(matrix_B)\
        .flatMap(Map_1)\
        .groupByKey()\
        .flatMap(Reduce_1)\
        .groupByKey()\
        .flatMap(Reduce_2)
    
    print(matrix_C.collect())

    spark.stop()