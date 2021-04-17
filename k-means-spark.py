import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from math import sqrt

spark = SparkSession.builder.appName("k-means").getOrCreate()
points = spark.read.text("punkty.txt").rdd.map(lambda x: tuple(x[0].split(" "))).zipWithIndex().map(lambda x: (x[1], (float(x[0][0]), float(x[0][1]))))

k = 3

centroid = points.filter(lambda a: a[0] < k)

iterations = 50

for _ in range(iterations):
    dist = points.cartesian(centroid)\
                .map(lambda x: (x[0], (x[1][0], sqrt((x[0][1][0] - x[1][1][0])**2 + (x[0][1][1] - x[1][1][1])**2))))\
                .reduceByKey(lambda a, b: a if a[1] < b[1] else b)\
                .map(lambda x: (x[1][0], (x[0], 1)))
    centroid = dist.reduceByKey(lambda a, b: ((a[0][0], (a[0][1][0] + b[0][1][0], a[0][1][1] + b[0][1][1])), a[1] + b[1]))\
                .map(lambda x: (x[0], (x[1][0][1][0]/x[1][1], x[1][0][1][1]/x[1][1])))

points = dist.map(lambda x: (x[1][0][1], x[0]))
draw_points = []
for i in range(k):
    draw_points.append(points.filter(lambda x: x[1] == i).map(lambda x: x[0]).collect())

centroid = centroid.map(lambda x: x[1]).collect()


for i in range(k):
    x, y = zip(*draw_points[i])
    plt.scatter(x, y)
    print(f"centroid: {centroid[i]}")
    print(draw_points[i])
    print("_____________")

x, y = zip(*centroid)
plt.scatter(x, y)

plt.show()

spark.stop()
