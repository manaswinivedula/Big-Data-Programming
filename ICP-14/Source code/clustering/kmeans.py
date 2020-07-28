from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import os

os.environ["SPARK_HOME"] = "C:\\spark-2.4.4-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"

spark = SparkSession.builder.appName("DecisionTree App").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Loading the data
data = spark.read.format("csv").option("header", True) .option("inferSchema", True) .option("delimiter", ",") \
                               .load("diabetic_data.csv")
data = data.select("admission_type_id", "discharge_disposition_id", "admission_source_id", "time_in_hospital", "num_lab_procedures")
data.show()

assembler = VectorAssembler(inputCols=data.columns, outputCol="features")
data = assembler.transform(data)
data.show()

# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(data)

# Make predictions
predictions = model.transform(data)
# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)