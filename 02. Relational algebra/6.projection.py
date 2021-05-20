from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import IntegerType

def Map(line):
    global column_numbers
    projection = ()
    for number in column_numbers:
        projection = projection + (line[number],)
    return [(line, projection)]


def Reduce(x):
    return [(x[0], tuple(x[1])[0])]


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Niepoprawna liczba argumentów", file=sys.stderr)
        exit(-1)

    spark = SparkSession.builder.appName("projection").getOrCreate()

    lines = spark.read.options(header = True, sep = ',').csv(sys.argv[1])

    headers = lines.columns

    columns = sys.argv[2].split(" ")
    column_numbers = [headers.index(column) for column in columns]

    selection = lines.rdd.map(tuple).flatMap(Map).groupByKey().flatMap(Reduce)
    
    for line in selection.collect():
        print(line)

    spark.stop()