from pyspark import SparkContext
import sys

outer = sys.argv[2]

def Map(line):
    w = line.split("\t")
    if w[0] == "Orders":
        return [(w[2], (w[0], w[1], w[3])),]
    elif w[0] == "Customers":
        return [(w[1], (w[0], w[2], w[3], w[4])),]
    return []

def Reduce(x):
    key, values = x[0], list(x[1])
    v = []

    global outer

    if outer.lower() == "left":
        for customer in values:
            if customer[0] == "Customers":
                matched = True
                for order in values:
                    if order[0] == "Orders":
                        v.append((customer[0], key, customer[1], customer[2], customer[3], order[0], order[1], key, order[2]))
                        matched = False
                if matched:
                    v.append((customer[0], key, customer[1], customer[2], customer[3], "NULL"))
        return v
    elif outer.lower() == "right":
        for order in values:
            if order[0] == "Orders":
                matched = True
                for customer in values:
                    if customer[0] == "Customers":
                        v.append((customer[0], key, customer[1], customer[2], customer[3], order[0], order[1], key, order[2]))
                        matched = False
                if matched:
                    v.append(("NULL", order[0], order[1], key, order[2]))
        return v
    return []


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkContext(appName="outer_join")

    lines = spark.textFile(sys.argv[1], 1)

    inner_join = lines.flatMap(Map).groupByKey().flatMap(Reduce)

    for line in inner_join.collect():
        print(line)

    spark.stop()