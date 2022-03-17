from time import time
import pyspark,re

#first part, loading into RDD
spark = pyspark.sql.SparkSession.builder.appName("Part B").getOrCreate()
rdd = spark.sparkContext.textFile("../access.log") #need to change this line later

#second part, parsing with Regex and Spark
tmp1 = rdd.flatMap(lambda x: x.split("-"))
tmp2 = tmp1.flatMap(lambda x: x.split("\""))
#tmp3 = tmp2.flatMap(lambda x:x.split(" "))
tmp3 = tmp2.filter(lambda x: x != ' ' and x != '')
tmp4 = tmp3.flatMap(lambda x: re.split(r"\s+",x,maxsplit=1))

#timestamps
print("printing timestamps")
timestamp = tmp4.filter(lambda x: re.match(r'^\[',x))
ftsmp = timestamp.map(lambda x: x.rstrip(" "))
print(ftsmp.take(10))

#host addresses
print("printing host addresses")
ha = tmp4.filter(lambda x: re.match(r'.+\..+\..+\..+',x) and re.match(r'^\d',x))
print(ha.take(10))

#request type
print("printing requests")
req = tmp4.filter(lambda x: re.match(r'[A-Z]+$',x))
print(req.take(10))



tmp5 = tmp4.filter(lambda x: re.match(r'^([\s\d]+)$',x))
tmp6 = tmp5.filter(lambda x: not re.match(r'^([\d]+)$',x))
sp1 = tmp6.flatMap(lambda x: re.split(r"\s+",x,maxsplit=1))

print("printing response codes")
codes = sp1.filter(lambda x: re.match(r'^([\d]+)$',x))
print(codes.take(10))

sp2 = sp1.filter(lambda x: not re.match(r'^([\d]+)$',x))
sp3 = sp2.flatMap(lambda x: x.split(" "))

print("printing response lengths")
reslen = sp3.filter(lambda x: x!= '')
print(reslen.take(10))



