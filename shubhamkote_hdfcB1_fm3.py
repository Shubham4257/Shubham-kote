#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os
os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/anaconda3/bin/python3.7'
os.environ['PYSPARK_PYTHON'] = '/opt/anaconda3/bin/python3.7'
os.environ["SPARK_HOME"] = '/opt/cloudera/parcels/CDH/lib/spark'
import findspark
findspark.init()
from pyspark import SparkContext , SparkConf
from pyspark.sql import SparkSession,SQLContext,Row

from pyspark.streaming import StreamingContext


# In[3]:


sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("Pysparkdf").getOrCreate()


# 1.Load moves.csv and cache the dataframe

# In[4]:


movies_df=spark.read.csv("movies.csv",sep=',',inferSchema=True,header=True)


# In[5]:


movies_df.persist()


# In[17]:


movies_df.show()


# 2.Load ratings.csv and cache the dataframe

# In[9]:


ratings_df=spark.read.csv("ratings.csv",sep=',',inferSchema=True,header=True)


# In[10]:


ratings_df.persist()


# In[11]:


ratings_df.show()


# 3.Find the number of records in movies dataframe

# In[15]:


movies_df.count()


# 4.Find the number of records in ratings dataframe

# In[16]:


ratings_df.count()


# In[23]:


ratings_df.select("userId").distinct().show()


# In[26]:


movies_df.createOrReplaceTempView("Movies")
ratings_df.createOrReplaceTempView("Ratings")


# In[27]:


Comb = spark.sql("SELECT * from Ratings inner join Movies on Ratings.userId=Movies.movieId ")


# In[28]:


Comb.show()


# In[43]:


spark.sql("SELECT avg(rating),count(*) from Rating group by movieId  ").show()


# In[47]:


spark.sql("SELECT max(rating) from Ratings group by movieId ").show()


# In[ ]:


spark.sql("SELECT movieId,avg(rating),count(rating) from Ratings")


# In[ ]:




