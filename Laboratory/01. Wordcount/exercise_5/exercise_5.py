from __future__ import print_function
import sys
import re
from operator import add
from pyspark.sql import SparkSession
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords

english_stopwords = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

def get_wordnet_pos(word):
    tag = nltk.pos_tag([word])[0][1][0].upper()
    tag_dict = {"J": nltk.corpus.wordnet.ADJ,
                "N": nltk.corpus.wordnet.NOUN,
                "V": nltk.corpus.wordnet.VERB,
                "R": nltk.corpus.wordnet.ADV}

    return tag_dict.get(tag, nltk.corpus.wordnet.NOUN)


def Map(r):
    key = re.sub(r'[^a-zA-Z]+', ' ', r[0].lower())
    return [ (lemmatizer.lemmatize(word, get_wordnet_pos(word)), 1)\
        for word in nltk.word_tokenize(key)\
            if word not in english_stopwords and len(word) > 1 ]


def Reduce(r):
    key, value = r[0], r[1]
    return (key, sum(value))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
            .builder\
            .appName("PythonWordCount")\
            .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd
    countsR = lines.flatMap(Map)\
            .groupByKey()\
            .sortBy(lambda line: line[0])\
            .map(Reduce)
            
    for result in countsR.collect():
        print(f"{result[0]}:\t{result[1]}")
        
    spark.stop()
