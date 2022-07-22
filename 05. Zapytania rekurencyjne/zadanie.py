from pyspark.sql import SparkSession
import sys


if __name__ == "__main__":

    spark = SparkSession.builder.appName("ZapytaniaRekurencyjne").getOrCreate()
    sparkContext = spark.sparkContext

    podpunkt = int(sys.argv[1])

    if podpunkt in {1, 3}:
        stacje_poczatkowe = sparkContext.parallelize([('YYZ', 'YYZ'), ("YTO", "YTO"), ("YTZ", "YTZ")])
    elif podpunkt in {2, 4}:
        stacje_poczatkowe = sparkContext.parallelize([('BZG', 'BZG'), ("WAW", "WAW")])
    
    records = spark.read.text("routes.csv").rdd\
        .map(lambda line: tuple(line[0].split(',')))\
        .map(lambda line: (line[2], line[4]))

    stacje_docelowe = stacje_poczatkowe

    if podpunkt in {1, 2, 3}:
        k = int(sys.argv[2])
        for i in range(k+1):
            stacje_docelowe = stacje_docelowe.join(records).flatMap(lambda x: [(x[1][0], x[1][0]), (x[1][1], x[1][1])]).distinct()

    elif podpunkt == 4:
        while True:
            pierwotna_liczba_miast = stacje_docelowe.count()
            stacje_docelowe = stacje_docelowe.join(records).flatMap(lambda x: [(x[1][0], x[1][0]), (x[1][1], x[1][1])]).distinct()
            nowa_liczba_miast = stacje_docelowe.count()

            if pierwotna_liczba_miast == nowa_liczba_miast:
                break
    
    stacje_docelowe = stacje_docelowe.collect()
    for stacja in stacje_poczatkowe.collect():
        if stacja in stacje_docelowe:
            stacje_docelowe.remove(stacja)

    nowa_liczba_miast = len(stacje_docelowe)

    print(stacje_docelowe)
    print(nowa_liczba_miast)