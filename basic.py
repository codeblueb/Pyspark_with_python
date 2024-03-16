import pyspark
import pandas as pd

from pyspark.sql import SparkSession

print(type(pd.read_csv('test1.csv')))

spark = SparkSession.builder.appName('Practice').getOrCreate()

df_ps = spark.read.csv('test1.csv')

df_ps = spark.read.option('header', 'true').csv('test1.csv')

print(type(df_ps))

print(df_ps.printSchema())
