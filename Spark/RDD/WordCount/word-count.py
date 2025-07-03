from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("Book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

with open("wordcount_output.txt", "w", encoding="utf-8") as f:
    for word, count in wordCounts.items():
        cleanWord = word.encode('ascii', 'ignore').decode()
        if cleanWord:
            f.write(f"{cleanWord} {count}\n")
