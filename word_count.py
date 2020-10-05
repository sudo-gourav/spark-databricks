# importing the necessary modules
from pyspark import SparkConf,SparkContext
import re
# creating a configuration object
config = SparkConf().setMaster("local").setAppName("Weather")

# creating a sparkcontext object
sc = SparkContext(conf = config)

def remove(entry):
	entry = entry.lower()

	return re.split("[^a-z]",entry)

# creating a rdd object from oliver_twist.txt
oliver_twist = sc.textFile('datasets/oliver_twist.txt').flatMap(lambda x:x.split()).flatMap(remove)

oliver_twist = oliver_twist.countByValue()

words = dict(sorted(oliver_twist.items()))

for word,countOfWord in words.items():
	print(word,countOfWord,sep = " ")
