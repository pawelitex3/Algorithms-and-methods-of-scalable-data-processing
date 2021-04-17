import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    countsR = lines.pipe("./mapper.py")\
        .sortBy(lambda line: line.split("\t")[0])\
        .pipe("./reducer.py")

    for line in countsR.collect():
        print("%s" % (line))

    spark.stop()
