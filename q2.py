import pyspark,re

def regex(x):

    host = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|-)'
    garbage = r'\s(.*|-)\s(.*|-)\s' #garbage hyphens - -
    timestamp =  r'(\[([0-9]{2}/[A-Za-z]{3}/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [-+][0-9]{4})\]|-)'
    requests = r'\s(\"(GET|POST|PUT|HEAD|DELETE|PATCH|OPTIONS|CONNECT|PROPFIND)\s(.*)"|-)' #googled these possible requests
    codes = r'\s(101|200|201|202|203|204|205|206|300|301|302|303|304|305|307|400|401|402|403|404|405|406|407|408|409|410|411|412|413|414|415|416|417|499|500|501|502|503|504|505)\s' #these are the possible response codes, googled that even 499 is possible
    reslen = r'(\d+|-)\s' #either a number or -

    p = re.search(host+garbage+timestamp+requests+codes+reslen,x)
    #group(1) is host
    #group(5) is timestamp
    #group(7) is request type
    #group(9) is response code
    #group(10) is response length
    if(p is None):
        return (None,None,None,None,None)
    elif(p.group(1) == "-" or p.group(5) == "-" or p.group(7) == "-" or p.group(9) == "-" or p.group(10) == "-"):
        return (None,None,None,None,None)
    else:
        return (p.group(1),p.group(5), p.group(7) ,p.group(9), p.group(10))

#first part, loading into RDD
spark = pyspark.sql.SparkSession.builder.appName("Part B").getOrCreate()
rdd = spark.sparkContext.textFile("../access.log") #need to change this line later
print(rdd.take(10))

tmp1 = rdd.map(lambda x: regex(x))
print(tmp1.take(10))
freaky = tmp1.toDF(['Remote Host','Request Timestamp','Request Method','Response Code','Response Length'])
rdd1 = freaky.rdd
print(rdd1.filter(lambda x: x[0] == None).count())
