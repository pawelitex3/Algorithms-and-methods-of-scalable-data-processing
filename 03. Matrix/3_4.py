from pyspark.sql import SparkSession
import sys

def Map(r):
    global n
    if r[0] == 'A':
        return [((r[1], i), (r[0], r[2], r[3])) for i in range(n)]
    elif r[0] == 'B':
        return [((i, r[2]), (r[0], r[1], r[3])) for i in range(n)]
    return []


def Reduce(r):
    sum = 0
    for A in r[1]:
        if A[0] == 'A':
            for B in r[1]:
                if B[0] == 'B':
                    if A[1] == B[1]:
                        sum += A[2]*B[2]
    if sum != 0:
        return [(r[0], sum)]
    return []


if __name__ == "__main__":

    global n
    spark = SparkSession.builder.appName("Matrix").getOrCreate()

    n = int(sys.argv[1])

    matrix_A = spark.read.text(sys.argv[2]).rdd\
        .map(lambda line: tuple(line[0].split(";")))\
        .map(lambda r: (r[0], int(r[1]), int(r[2]), float(r[3])))

    matrix_B = spark.read.text(sys.argv[3]).rdd\
        .map(lambda line: tuple(line[0].split(";")))\
        .map(lambda r: (r[0], int(r[1]), int(r[2]), float(r[3])))

    matrix_C = matrix_A.union(matrix_B).flatMap(Map).groupByKey().flatMap(Reduce)
    
    print(matrix_C.collect())

    spark.stop()