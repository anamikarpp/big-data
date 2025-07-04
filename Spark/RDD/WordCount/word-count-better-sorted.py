import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("Book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

# Save results to file
with open("wordcount_sorted_output.txt", "w", encoding="utf-8") as f:
    for result in results:
        count = str(result[0])
        word = result[1].encode('ascii', 'ignore').decode()
        if word:
            f.write(f"{word}:\t\t{count}\n")

print("Word count results saved to 'wordcount_sorted_output.txt'")