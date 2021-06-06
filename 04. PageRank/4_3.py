from pyspark.sql import SparkSession
import numpy as np
import sys
import os


slice_lenght = 2

def Map(r):
    global current_vector_slice, current_slice_number
    vertex, neighbours_num = r[0], r[1]
    vertex, neighbours_num = int(vertex), int(neighbours_num)

    if vertex//slice_lenght+1 != current_slice_number:
        current_slice_number = vertex//slice_lenght+1
        vector_path = f"vector_{current_slice_number}"
        read_vector(vector_path)

    neighbours = []
    for i in range(2, len(r)):
        neighbours.append(int(r[i]))
    return [(neighbour, current_vector_slice[vertex]/neighbours_num) for neighbour in neighbours]


def Reduce(r):
    global S, beta
    key, value = r[0], r[1]
    value = sum(value)*beta
    if key in S:
        return (key, value + (1.0-beta))
    else:
        return (key, value)


def fraction_to_float(fraction):
    try:
        return float(fraction)
    except:
        numerator, denominator = fraction.split("/")
        return float(float(numerator)/float(denominator))


def read_vector(vector_path):
    global current_vector_slice
    current_vector_slice = dict()
    vector_file = open(vector_path, "r")
    lines = vector_file.readlines()
    for line in lines:
        line = line.split(";")
        current_vector_slice[int(line[0])] = float(line[1])

def save_vector(new_vector):
    global n
    vector = [ [] for _ in range(n//slice_lenght) ]
    for key in new_vector:
        vector[key//slice_lenght].append((key, new_vector[key]))

    for i in range(len(vector)):
        vector_path = f"vector_{i+1}"
        vector_file = open(vector_path, "w")
        for line in vector[i]:
            vector_file.write(f"{line[0]};{line[1]}\n")


def prepare_matrix(matrix, path):
    global n, files_list
    neighbours_number = np.zeros(n)
    neighbours_list = [ [] for i in range(n) ]

    for cell in matrix:
        neighbours_number[cell[1]] += 1
        neighbours_list[cell[1]].append(cell[0])

    for i in range(n):
        col = i//slice_lenght
        blocks = [ [] for _ in range(n//slice_lenght)]
        for neighbour in neighbours_list[i]:
            block = neighbour//slice_lenght
            blocks[block].append(neighbour)
        for row in range(n//slice_lenght):
            if blocks[row] != []:
                new_path = f"{path}_block_{row+1}{col+1}"
                line = f"{i} {int(neighbours_number[i])}"
                for neighbour in blocks[row]:
                    line += f" {neighbour}"
                line += "\n"
                files_list.add(new_path)
                with open(new_path, "a") as output:
                    output.write(line)


def remove_old_files(path):
    for f in os.listdir('.'):
        if f.startswith(path + "_") or f.startswith("vector_"):
            os.remove(f)


if __name__ == "__main__":

    global beta, S, n, files_list, current_slice_number, current_vector_slice
    spark = SparkSession.builder.appName("PageRank").getOrCreate()

    files_list = set()
    n = int(sys.argv[2])
    beta = 0.8
    S = {0}
    current_slice_number = -1

    current_vector_slice = dict()
    for i in range(n):
        current_vector_slice[i] = 1/n

    adjacency_matrix = spark.read.text(sys.argv[1]).rdd\
        .map(lambda line: tuple(line[0].split(";")))\
        .map(lambda r: (int(r[0]), int(r[1]), int(r[2])))

    remove_old_files(sys.argv[1])
    prepare_matrix(adjacency_matrix.collect(), sys.argv[1])
    save_vector(current_vector_slice)

    matrix = spark.read.text(list(files_list)).rdd\
        .map(lambda line: tuple(line[0].split()))\

    print(current_vector_slice)

    for i in range(50):
        new_vector = matrix.flatMap(Map).groupByKey().map(Reduce)
        vector = new_vector.collectAsMap()
        print(vector)
        save_vector(vector)
        current_slice_number = -1

    spark.stop()