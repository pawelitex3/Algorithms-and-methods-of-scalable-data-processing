from pyspark import SparkContext
import sys


def Map(line):
    w = line.split("\t")
    if len(w) == 2:
        return [(w[1], w[1]),]
    return []

def Reduce(x):
    key, value = x[0], list(x[1])
    if len(value) == 2:
        return [key]
    return []

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkContext(appName="intersection")

    lines = spark.textFile(sys.argv[1], 1)

    intersection = lines.flatMap(Map).groupByKey().flatMap(Reduce)

    for line in intersection.collect():
        print(line)

    spark.stop()