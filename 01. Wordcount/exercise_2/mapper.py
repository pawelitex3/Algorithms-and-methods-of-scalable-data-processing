#!/usr/bin/env python3
from sys import stdin
import re
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords, wordnet

def get_wordnet_pos(word):
    tag = nltk.pos_tag([word])[0][1][0].upper()
    tag_dict = {"J": wordnet.ADJ,
                "N": wordnet.NOUN,
                "V": wordnet.VERB,
                "R": wordnet.ADV}

    return tag_dict.get(tag, wordnet.NOUN)


if __name__ == "__main__":

    english_stopwords = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()

    for line in stdin:
        line = re.sub(r'[^a-zA-Z]+', ' ', line).lower()
        for word in nltk.word_tokenize(line):
            if word not in english_stopwords and len(word) > 1:
                print(f"{lemmatizer.lemmatize(word, get_wordnet_pos(word))}\t1")

    
