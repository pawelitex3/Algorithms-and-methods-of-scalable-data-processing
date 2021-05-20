from pyspark import SparkContext
import sys


def Map(line):
    w = line.split("\t")
    if w[0] == "Orders":
        return [(w[2], (w[0], w[1], w[3])),]
    elif w[0] == "Customers":
        return [(w[1], (w[0], w[2], w[3], w[4])),]
    return []

def Reduce(x):
    key, values = x[0], list(x[1])
    if len(values) > 1:
        v = []
        for customer in values:
            if customer[0] == "Customers":
                for order in values:
                    if order[0] == "Orders":
                        v.append((customer[0], key, customer[1], customer[2], customer[3], order[0], order[1], key, order[2]))
        return v
    return []

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Niepoprawna liczba argumentów", file=sys.stderr)
        exit(-1)

    spark = SparkContext(appName="inner_join")

    lines = spark.textFile(sys.argv[1], 1)

    inner_join = lines.flatMap(Map).groupByKey().flatMap(Reduce)

    for line in inner_join.collect():
        print(line)

    spark.stop()