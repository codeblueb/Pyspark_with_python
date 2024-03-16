
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Agg').getOrCreate()

df_ps = spark.read.csv('test3.csv', header=True, inferSchema=True)

# df_ps.show()

# Groupby 
# Groupby to find the maximum salary
df_ps.groupby('Name').sum().show()

df_ps.groupby('Name').avg().show()

# Groupby Departments which gives maximum salary
df_ps.groupby('Departments').sum().show()

df_ps.groupby('Departments').mean().show()

df_ps.groupby('Departments').count().show()

df_ps.agg({'Salary':'sum'}).show()

