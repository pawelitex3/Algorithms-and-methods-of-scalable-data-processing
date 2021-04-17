#!/usr/bin/env python3
from subprocess import Popen, PIPE
from shlex import split
import sys

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    p1 = Popen(split(f"cat {sys.argv[1]}"), stdout=PIPE)
    p2 = Popen(split("./mapper.py"), stdin=p1.stdout, stdout=PIPE)
    p3 = Popen(split("sort"), stdin=p2.stdout, stdout=PIPE)
    p4 = Popen(split("./reducer.py"), stdin=p3.stdout)
    