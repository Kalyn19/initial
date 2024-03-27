from pyspark.sql import SparkSession
import os,json,sys
import argparse, gcsfs

'''
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument("jobinputs", ..., required=True)
parser.parse_args()
'''

#jobinputs={}
gcs_file_system = gcsfs.GCSFileSystem(project="mar2024project")
gcs_json_path = "gs://mar2024bkt/sample.json"
with gcs_file_system.open(gcs_json_path) as f:
  jobinputs = json.load(f)


print("jobinputs")
print(jobinputs)
bq_table_id=jobinputs['project_id']+"."+jobinputs['bqdataset_name']+"."+jobinputs['bqtable_name']

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .config("mapreduce.input.fileinputformat.input.dir.recursive","True") \
      .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.15.1-beta') \
      .enableHiveSupport() \
      .getOrCreate() 

dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd=spark.sparkContext.parallelize(dataList)
print("datalist is :")
print(dataList)
print("rdd is ")
print(rdd)

sql_query="CREATE EXTERNAL TABLE IF NOT EXISTS "+jobinputs['hdfs_table_name'] \
 +"(movieid integer, title varchar(1000),genre varchar(1000)) \
row format delimited \
fields terminated by ',' stored as textfile LOCATION 'gs://"+jobinputs['bucket_name']+"/movies/'"
#.format(jobinputs['hdfs_table_name'],jobinputs['bucket_name'])

print("SQL QUERY IS : "+sql_query)

spark.sql(sql_query).show(1000,False)

sampledata_query="select * from "+jobinputs['hdfs_table_name']+" limit 20"
sampledata=spark.sql(sampledata_query)

print("Sample Data is: ")
print(sampledata)

sampledata.write \
.format("bigquery") \
.option('table', bq_table_id) \
.option("temporaryGcsBucket",jobinputs['bucket_name']) \
.option("encoding", "UTF-8") \
.option("nullValue", "\u0000") \
.option("emptyValue", "\u0000") \
.mode("append") \
.save()

