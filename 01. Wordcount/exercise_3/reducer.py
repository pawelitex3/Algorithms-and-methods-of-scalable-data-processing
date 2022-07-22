#!/usr/bin/env python3
from sys import stdin

if __name__ == "__main__":

    words = {}

    for line in stdin:
        word, count = line.split()
        if word not in words:
            words[word] = int(count)
        else:
            words[word] += int(count)
        
    for word, count in words.items():
        print(f"{word}\t{count}")
        