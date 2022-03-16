import pyspark,csv
from collections import defaultdict

def def_value():
    return 0

spark = pyspark.sql.SparkSession.builder.appName("Spark Lab").getOrCreate()
#print(spark.version())
d = defaultdict(def_value)
p = []
cases = spark.read.load("groceries - groceries.csv",format="csv", sep=",", inferSchema="true", header="true")

temp = cases.collect()
for i in range(len(temp)):
    for x in range(temp[i]['Item(s)']):
        for y in range(x):
            s1 = 'Item ' + str(x+1)
            s2 = 'Item ' + str(y+1)
            if(d[(temp[i][s1],temp[i][s2])] > 0):
                d[(temp[i][s1],temp[i][s2])] += 1
            elif(d[(temp[i][s2],temp[i][s1])] > 0):
                d[(temp[i][s2],temp[i][s1])] += 1
                del d[(temp[i][s1],temp[i][s2])]
            else:
                p.append((temp[i][s1],temp[i][s2]))
                d[(temp[i][s1],temp[i][s2])] += 1
                del d[(temp[i][s2],temp[i][s1])]

#first part
rdd1=spark.sparkContext.parallelize(p)

#second part
s = sorted(d.items(), key = lambda i: i[1],reverse=True)
rdd2=spark.sparkContext.parallelize(s)
#print(s)
header = ['item1','item2','count']
forCSV = []
for item in s:
    l = []
    l.append(item[0][0])
    l.append(item[0][1])
    l.append(item[1])
    forCSV.append(l)
with open('count.csv','w') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    for l in forCSV:
        writer.writerow(l)

#third part
rdd3 = rdd2.take(5)
for row in rdd3:
    print(row[0])
    