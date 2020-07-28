from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
import os

os.environ["SPARK_HOME"] = "C:\\spark-2.4.4-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"

spark = SparkSession.builder.appName("LinerReg App").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Loading the data
data = spark.read.format("csv").option("header", True) \
                               .option("inferSchema", True) \
                               .option("delimiter", ",") \
                               .load("imports-85.data")


data.printSchema()

data = data.withColumnRenamed("wheel-base","label").select("label", "length", "width", "height")
data.show()

assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
data = assembler.transform(data)
data.show()


lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
model = lr.fit(data)

# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(model.coefficients))
print("Intercept: %s" % str(model.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = model.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)