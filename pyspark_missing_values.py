"""
    Handling missing values
     - Dropping columns
     - Dropping rows
     - Various paramter in dropping functionalities
     - Handling missing values by Mean, Median, Mode
"""
# import distutils
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practice').getOrCreate()

# explains why [ distutils ] is depreciated 
# Below module uses distutils as an dependacy but you hava to make
# sure you are using an virtual enviornment and then you will have 
# to pip install setuptools to use the module below
# https://peps.python.org/pep-0632/#migration-advice
from pyspark.ml.feature import Imputer

df_ps = spark.read.csv('test2.csv', header=True, inferSchema=True)

df_ps.printSchema()

df_ps.show()

df_ps.drop('Name').show()

df_ps.na.drop().show()

# any == how
df_ps.na.drop(how='any').show()

# threshold if there are 3 slumns that is NULL then drop them
df_ps.na.drop(how='any', thresh=3).show()

df_ps.na.drop(how='any', subset=['Age']).show() # if there are null in the column Age
df_ps.na.drop(how='any', subset=['Name']).show() # if there are nulls in the column Name

df_ps.na.fill('Missing', ['Experience', 'age']).show() # this is not working we so need to figure out a way

imputer = Imputer(
        inputCols = ['age', 'Experience', 'Salary'],
        outputCols = ["{}_imputed".format(c) for c in ['age', 'Experience', 'Salary']]
    ).setStrategy("median")

imputer.fit(df_ps).transform(df_ps).show()

