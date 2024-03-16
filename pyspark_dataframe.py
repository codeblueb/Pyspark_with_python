# Pyspark Dataframe
# Reading the Dataset
# Checking the Datatypes of the Column(Schema)
# Selecting Columns and Indexing 
# Check Describe option similar to Pandas
# Adding Columns
# Dropping Columns
# Renaming Columns

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Dataframe').getOrCreate()

# reading the dataset
df_ps = spark.read.option('header', 'true').csv('test1.csv', inferSchema=True)

# checking the schema 
print(df_ps.printSchema())

df_ps = spark.read.csv('test1.csv', header=True, inferSchema=True)

print(df_ps.show())

# df_ps.head(3) # This will return a list 

print(df_ps.select(['Name', 'Experience']).show()) # selecting columns 

# df_ps['Name'] 
# df_ps.dtypes

# print(df_ps.describe().show())

# adding columns in the dataframe
df_ps = df_ps.withColumn('Experience After 2 years', df_ps['Experience'] + 2)

print(df_ps.show())

# Now drop the column
df_ps = df_ps.drop('Experience After 2 years')

print(df_ps.show())

print(df_ps.withColumnRenamed('Name', 'New Name').show())
