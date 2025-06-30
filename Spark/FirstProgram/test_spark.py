from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("SimpleApp").setMaster("local[*]")
sc = SparkContext(conf=conf)

nums = sc.parallelize([1, 2, 3, 4, 5])
squared = nums.map(lambda x: x * x)
result = squared.collect()

print(result)

sc.stop()
