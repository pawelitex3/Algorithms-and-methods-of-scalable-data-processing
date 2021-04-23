from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import IntegerType
import re

def tryint(x):
    try:
        return int(x)
    except ValueError:
        return x

def Map(line):
    global headers, column, operation, value
    index = headers.index(column)
    toCompare = tryint(line[index])
    if operation == "==":
        if toCompare == value:
            return [(line, line)]
        else:
            return []
    elif operation == "<":
        if toCompare < value:
            return [(line, line)]
        else:
            return []
    elif operation == "<=":
        if toCompare <= value:
            return [(line, line)]
        else:
            return []
    elif operation == ">":
        if toCompare > value:
            return [(line, line)]
        else:
            return []
    elif operation == ">=":
        if toCompare >= value:
            return [(line, line)]
        else:
            return []
    elif operation == "!=":
        if toCompare != value:
            return [(line, line)]
        else:
            return []
    else:
        return []

def Reduce(x):
    return [x[0]]

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession.builder.appName("selection").getOrCreate()

    lines = spark.read.options(header = True, sep = ',').csv(sys.argv[1])

    headers = lines.columns

    condition = sys.argv[2].split(" ")

    column = condition[0]
    operation = condition[1]
    value = tryint(condition[2])

    selection = lines.rdd.map(tuple).flatMap(Map).groupByKey().flatMap(Reduce)

    
    for line in selection.collect():
        print(line)

    spark.stop()