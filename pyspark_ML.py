
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Missing').getOrCreate()

training = spark.read.csv('test1.csv', header=True, inferSchema=True)

training.show()
print(training.columns)

from pyspark.ml.feature import VectorAssembler

featureassembler = VectorAssembler( inputCols=['age', 'Experience'], outputCol = "Independent Features" )

output = featureassembler.transform(training)

output.show()

finalized_data = output.select("Independent Features", "Salary")

finalized_data.show()

from pyspark.ml.regression import LinearRegression

# train test split
train_data, test_data = finalized_data.randomSplit( [0.75, 0.25] )

regressor = LinearRegression(featuresCol='Independent Features', labelCol='Salary')

regressor = regressor.fit(train_data)

print(regressor.coefficients)

print(regressor.intercept)

# Prediction
predict_result = regressor.evaluate(test_data)

predict_result.predictions.show()

print(predict_result.meanAbsoluteError)
print(predict_result.meanSquaredError)
