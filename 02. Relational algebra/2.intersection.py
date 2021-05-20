from pyspark import SparkContext
import sys


def Map(line):
    w = line.split()
    if len(w) == 2:
        return [(int(w[1]), int(w[1])),]
    return []

def Reduce(x):
    key, value = x[0], list(x[1])
    if len(value) == 2:
        return [(key, key)]
    return []

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Niepoprawna liczba argumentów", file=sys.stderr)
        exit(-1)

    spark = SparkContext(appName="intersection")

    lines = spark.textFile(sys.argv[1], 1)

    intersection = lines.flatMap(Map).groupByKey().flatMap(Reduce)

    for line in intersection.collect():
        print(line)

    spark.stop()