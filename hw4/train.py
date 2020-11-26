
# coding: utf-8

# In[1]:


import sys
import os
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


# In[2]:


datasetPath = 'gs://hw4_spark_b06902127/data_banknote_authentication.txt'
iteration = 1000
D = 5

w = 2 * np.random.ranf(size=D) - 1
lr = 0.001


# In[33]:


def readPointBatch(iterator):
    strs = list(iterator)
    matrix = np.ones((len(strs), D + 1))
    for i, s in enumerate(strs):
        matrix[i, :-1] = np.fromstring(s.replace(',', ' '), dtype=np.float32, sep=' ')
        label = matrix[i, 4]
        matrix[i, 4] = matrix[i, 0]
        matrix[i, 0] = label
    return matrix


# In[34]:


spark = SparkSession.builder.appName("spark").getOrCreate()
points = spark.read.text(datasetPath).rdd.map(lambda r: r[0]).mapPartitions(readPointBatch).cache()


# In[91]:


def sigmoid(x):
    return 1.0/(np.exp(-x) + 1.0)

def gradient(matrix, w):
    Y = matrix[:, 0]
    X = matrix[:, 1:]
    return -(X.T.dot(Y - sigmoid(X.dot(w))))

def add(x, y):
    return x + y

def predict(matrix, w):
    X = matrix[:, 1:]
    Y = matrix[:, 0]
    result = sigmoid(X.dot(w))
    return result, Y


# In[52]:


for i in range(iteration):
    grad = points.map(lambda m: gradient(m, w)).reduce(add)
    w -= lr*grad


# In[102]:


result = points.map(lambda m: predict(m, w))
pred, label = result.collect()[0]
acc = 0.
for i in range(len(pred)):
    p = pred[i]
    l = label[i]
    if p > 0.5 and l > 0:
        acc += 1
    elif p <= 0.5 and l < 1:
        acc += 1
print acc/len(pred)


# In[103]:


print(w)

