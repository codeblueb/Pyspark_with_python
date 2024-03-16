"""
    Pyspark dataframe
     - Filter Operations
     - &, |, ==
     - ~
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Dataframe').getOrCreate()

df_ps = spark.read.csv('test1.csv', header=True, inferSchema=True)

# filter : salary of the people less than or equal to 20000
df_ps.filter("Salary<=20000").show()

df_ps.filter("Salary<=20000").select(['Name', 'age']).show()

df_ps.filter(df_ps['Salary']<= 20000).show()

df_ps.filter((df_ps['Salary']<=20000) |
             (df_ps['Salary']>=15000)
    ).show()

df_ps.filter(~(df_ps['Salary']<=20000)).show()
