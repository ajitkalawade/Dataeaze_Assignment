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



#QUestion No. 1
print("Answer of FIrst Question: ")
file3.createOrReplaceTempView("v1")
#print(file3.filter(file3.State=='VA').count())
sql = "SELECT count(*)FROM v1 where State= 'VA' "
result = spark.sql(sql)
print(result.show())



#Question No. 2
print("Answer of second Question: ")

sql = "select count(*),OwnerTypes from v1 where  State= 'VA' group by OwnerTypes"
result = spark.sql(sql)
print(result.show())

#QUestion No 3

print("Answer of third Question: ")
from pyspark.sql.types import IntegerType

df4=file3.withColumn('GrossSqFoot',file3['GrossSqFoot'].cast(IntegerType()))

df4.registerTempTable('df4_table')

spark.sql("select sum(GrossSqFoot) from df4_table where State=='VA' and IsCertified=='Yes' ").show()


#Question No 4

print("Answer of fourth Question: ")
group_zip_code=file3.filter(file3.State=='VA').groupby('Zipcode').count().show()

from pyspark.sql.types import IntegerType

#df4=file3.withColumn('GrossSqFoot',file3['GrossSqFoot'].cast(IntegerType()))

#df4.registerTempTable('df4_table')
#spark.sql("select Zipcode,count(*) from df4_table where State=='VA' group by Zipcode ").show()



#print(file3)




