from pyspark import SparkContext
import sys


def Map(line):
    w = line.split()
    if len(w) == 2:
        return [(int(w[1]), int(w[1])),]
    return []

def Reduce(x):
    key, value = x[0], x[1]
    return (key, key)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Niepoprawna liczba argumentów", file=sys.stderr)
        exit(-1)

    spark = SparkContext(appName="union")

    lines = spark.textFile(sys.argv[1], 1)

    union = lines.flatMap(Map).sortByKey().groupByKey().map(Reduce)

    for line in union.collect():
        print(line)

    spark.stop()