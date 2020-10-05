# importing the necessary modules
from pyspark import SparkConf,SparkContext

# creating a configuration object 
config = SparkConf().setMaster("local").setAppName("BinaryBook")

# creating a sparkcontext object
sc = SparkContext(conf = config)

# defining color vars
white,gray,black = 0,1,2 # => 0 < 1 < 2 

# defining function for getting user metadata 
def getMetadata():
	metaDict = {} 
	with open('datasets/user_master.txt') as metadataFile:
		contentsOfFile = metadataFile.read()
	contentsOfFile = contentsOfFile.split("\n")
	for each_entry in contentsOfFile:
		metaDict[str(each_entry.split(",")[0])] = str(each_entry.split(",")[1])
	return metaDict	

# defining user inputs
startID = "bin0001" # Dennis Ritchie
 										# C loves JAVA
targetID = "bin1000" # James Gosling 

isReached  = sc.accumulator(0) # this is to identify whether we have reached the intended target or not
lookup  = sc.broadcast(getMetadata())

# defining function to format the contents of rdd
'''
	(id,(color,distance,list of connections))

'''
def formatContent(entry):
	userID = entry.split(",")[0]
	connections = entry.split(",")[1:]
	if userID == startID:
		color = gray
		distance = 0
	else:
		color = white
		distance = 99999
	return (userID,(color,distance,connections))		 

# defining function to traverse through the graph:
def traversal(nodes):
	userID = nodes[0]
	color = nodes[1][0]
	distance = nodes[1][1]
	connections = nodes[1][2]
	newEntries = []
	if color == gray:
		for connection in connections:
			if connection == targetID:
				isReached.add(1)
			newEntries.append((connection,(gray,distance+1,[])))
		color = black
	else:
		for connection in connections:
			newEntries.append((connection,(white,distance,[])))

	newEntries.append((userID,(color,distance,connections)))
	return newEntries
			


# read the contents of binary_connections as rdd
connections = sc.textFile('datasets/binary_connections.txt').map(formatContent)


# deg(sep) >=1 and deg(sep) <=6 
# Every programmer on BinaryBook is connected to each other by at most 6 degrees of separation

for iteration in range(1,7):
	print("Interation: {}".format(iteration))
	connections = connections.flatMap(traversal)
	connections = connections.reduceByKey(lambda x,y: (max(x[0],y[0]),min(x[1],y[1]),x[2]+y[2] ))
	connections.collect()
	if isReached.value > 0:
		print("Degree of separation between {} and {} is {}".format(lookup.value[startID],lookup.value[targetID],iteration))
		break
	