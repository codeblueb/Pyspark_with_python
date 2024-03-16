
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Regressor').getOrCreate()

df_ps = spark.read.csv('tips.csv', header=True, inferSchema=True)

df_ps.show()

# handling Categorical features
from pyspark.ml.feature import StringIndexer

indexer = StringIndexer( inputCol='sex', outputCol='sex_indexed' )

df_r = indexer.fit(df_ps).transform(df_ps)

df_r.show()

indexer = StringIndexer( inputCols = ["smoker", "day", "time"],
                         outputCols = ["smoker_indexed", "day_indexed", "time_indexed"])

df_r = indexer.fit(df_r).transform(df_r)
df_r.show()

from pyspark.ml.feature import VectorAssembler

feature_assembler = VectorAssembler( inputCols = [
                        'tip', 'size', 'sex_indexed', 'smoker_indexed', 'day_indexed', 'time_indexed'    
                    ], outputCol = "Independent Features")

output = feature_assembler.transform(df_r)

output.show()

finalized_data = output.select("Independent Features", "total_bill")

finalized_data.show()

from pyspark.ml.regression import LinearRegression

# train test split
train_data, test_data = finalized_data.randomSplit([0.75, 0.25])

regressor = LinearRegression( featuresCol = "Independent Features", labelCol = "total_bill" )

regressor = regressor.fit(train_data)

print(regressor.coefficients, regressor.intercept)

# prediction
predict_results = regressor.evaluate(test_data)

# Final comparison
predict_results.predictions.show()

print(predict_results.r2, predict_results.meanAbsoluteError, predict_results.meanSquaredError)



