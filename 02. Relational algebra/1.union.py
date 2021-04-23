from pyspark import SparkContext
import sys


def Map(line):
    w = line.split("\t")
    if len(w) == 2:
        return [(w[1], w[1]),]
    return []

def Reduce(x):
    key, value = x[0], x[1]
    return key

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkContext(appName="union")

    lines = spark.textFile(sys.argv[1], 1)

    union = lines.flatMap(Map).groupByKey().map(Reduce)

    for line in union.collect():
        print(line)

    spark.stop()