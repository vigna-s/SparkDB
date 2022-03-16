from time import time
import pyspark,re

#first part, loading into RDD
spark = pyspark.sql.SparkSession.builder.appName("Part B").getOrCreate()
rdd = spark.sparkContext.textFile("../access.log")

#second part, parsing with Regex and Spark
tmp1 = rdd.flatMap(lambda x: x.split("-"))
tmp2 = tmp1.flatMap(lambda x: x.split("\""))
#tmp3 = tmp2.flatMap(lambda x:x.split(" "))
tmp3 = tmp2.filter(lambda x: x != ' ' and x != '')
print(tmp3.take(20))
tmp4 = tmp3.flatMap(lambda x: re.split(r"\s+",x,maxsplit=1))
print(tmp4.take(20))

#timestamps
print("printing timestamps")
timestamp = tmp4.filter(lambda x: re.match(r'^\[',x))
ftsmp = timestamp.map(lambda x: x.rstrip(" "))
print(ftsmp.take(10))

#host addresses
print("printing host addresses")
ha = tmp4.filter(lambda x: re.match(r'.+\..+\..+\..+',x) and re.match(r'^\d',x))
print(ha.take(10))

tmp5= tmp4.filter(lambda x: x != ' ' and x != '')
#print(tmp5.take(20))
tmp6 = tmp5.filter(lambda x: not re.match(r'.+\..+\..+\..+',x) and re.match(r'^\d',x))
print(tmp6.take(20))

sp1 = tmp6.flatMap(lambda x: x.split(" "))
print(sp1.take(20))
sp2 = sp1.filter(lambda x: x!='')

# tmp6 = tmp5.filter(lambda x: not re.match(r'^\/',x))
# print(tmp6.take(20))
# tmp7 = tmp6.filter(lambda x: not re.match(r'^\(',x))
# print(tmp7.take(20))
# tmp8 = tmp7.filter(lambda x: not re.match(r'^http',x))
# print(tmp8.take(20))



