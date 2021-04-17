#!/usr/bin/env python3
from sys import stdin

if __name__ == "__main__":

    for line in stdin:
        words = line.split()
        for word in words:
            print(f"{word}\t1")
