import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .config("spark.sql.shuffle.partitions", "2")\
    .appName("demo04")\
    .master("local[2]")\
    .getOrCreate()


file1=spark.read.csv("/home/sunbeam/Downloads/nonConfidential.csv",inferSchema=True,header=True)


file2=spark.read.parquet("/home/sunbeam/Downloads/confidential.snappy.parquet",inferSchema=True,header=True)


#file2=spark.read.parquet("/home/sunbeam/Downloads/confidential.snappy.parquet",header=True)
#print(file2.printSchema())


file3=file1.unionAll(file2)

#owner_type=df3.filter(df3.State=='VA').groupby('OwnerTypes').count().show()







#Question No. 2
print("Answer of second Question: ")


file3.createOrReplaceTempView("v1")

sql = "select count(*),OwnerTypes from v1 where  State= 'VA' group by OwnerTypes"
result = spark.sql(sql)
print(result.show())





