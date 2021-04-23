from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import IntegerType
import re

def tryint(x):
    try:
        return int(x)
    except ValueError:
        return x


def tryfloat(x):
    try:
        return float(x)
    except ValueError:
        return x


def Map(line):
    global column_numbers
    projection = ()
    for number in column_numbers:
        if number == len(line)-1:
            projection = projection + (tryfloat(line[number][1:]),)
        else:
            projection = projection + (tryint(line[number]),)
    return [projection]


def Reduce(x):
    global function
    key, value = x[0], x[1]
    if function == "count":
        return [(key, len(value))]
    elif function == "min":
        return [(key, min(value))]
    elif function == "max":
        return [(key, max(value))]
    elif function == "avg":
        return [(key, round(sum(value)/len(value), 2))]
    elif function == "sum":
        return [(key, round(sum(value), 2))]
    return []


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession.builder.appName("selection").getOrCreate()

    lines = spark.read.options(header = True, sep = ',').csv(sys.argv[1])

    headers = lines.columns

    columns = sys.argv[2].split(" ")
    column_numbers = [headers.index(column) for column in columns[:-1]]
    function = columns[-1]



    selection = lines.rdd.map(tuple).flatMap(Map).groupByKey().flatMap(Reduce)
    
    for line in selection.collect():
        print(line)

    spark.stop()