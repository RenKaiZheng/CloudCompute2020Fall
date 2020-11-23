import sys
import os
SPARK_HOME = "/usr/local/spark-0.9.1"
os.environ["SPARK_HOME"] = SPARK_HOME
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
sys.path.append(SPARK_HOME + "/python")


from pyspark.mllib.classification import LogisticRegressionWithSGD
import numpy as np
from pyspark import SparkConf, SparkContext


def getSparkContext():
    """
    Gets the Spark Context
    """
    conf = (SparkConf()
         .setMaster("local") # run on local
         .setAppName("Logistic Regression") # Name of App
         .set("spark.executor.memory", "1g")) # Set 1 gig of memory
    sc = SparkContext(conf = conf) 
    return sc

def mapper(line):
    """
    Mapper that converts an input line to a feature vector
    """    
    feats = line.strip().split(",") 
    # labels must be at the beginning for LRSGD
    label = feats[len(feats) - 1] 
    feats = feats[: len(feats) - 1]
    feats.insert(0,label)
    features = [ float(feature) for feature in feats ] # need floats
    return np.array(features)

sc = getSparkContext()

# Load and parse the data
data = sc.textFile("hdfs://localhost/user/hduser2/data")
parsedData = data.map(mapper)

'''
data = spark.textFile(...).map(readPoint).cache()
w = Vector.random(D)
for (i <- 1 to ITERATIONS) {
    val gradient = data.map(p =>
        (1 / (1 + exp(-p.y*(w dot p.x))) - p.y) * p.y * p.x
    ).reduce(_ + _)
    w -= gradient
}
'''

# Train model
model = LogisticRegressionWithSGD.train(parsedData)

# Predict the first elem will be actual data and the second 
# item will be the prediction of the model
labelsAndPreds = parsedData.map(lambda point: (int(point.item(0)), 
        model.predict(point.take(range(1, point.size)))))

# Evaluating the model on training data
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())

'''
threshold = 0.5
def sigmoid(x):
    return 1.0/(np.exp(-x) + 1.0)
for i in range(10000):
    gradient = -(train_x.T.dot(train_y - sigmoid(train_x.dot(w))))
    w = w - lr*gradient#/(np.sqrt(ada_sum) + 0.00000001)
'''

# Print some stuff
print("Training Error = " + str(trainErr))