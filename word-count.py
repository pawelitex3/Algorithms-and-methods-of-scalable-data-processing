from collections import Counter


if __name__ == "__main__":
    book = open("book.txt", "r")
    text = book.read().lower()

    for

    all_words = text.split(" ")
    Counter(all_words).most_common()
    for word, count in Counter(all_words).most_common():
        print(f"{word}: {count}")
