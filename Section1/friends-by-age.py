from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///SparkCourse/Section1/fakefriends.csv")
rdd = lines.map(parseLine)  #has the key value pair for age:number of friends
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#(23,(56,1)),(23,(10,1))--> (23,(56+10,1+1))--> (23,(66,2))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
#(23,(56/2))-->(23,28)
results = averagesByAge.collect()
for result in results:
    print(result)
