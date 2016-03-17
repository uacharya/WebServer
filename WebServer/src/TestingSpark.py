'''
Created on Mar 10, 2016

@author: walluser
'''

from pyspark import SparkConf,SparkContext;
import json,operator;
import os


def make_part_filter(index):
    
    

if __name__ == '__main__':
    
    #configure the spark environment
    sparkConf = SparkConf().setAppName("WordCount").setMaster("local[4]")
    sc = SparkContext(conf = sparkConf);
    
    
#     #The wordcount Spark Program
#     text_file = sc.textFile(os.environ['SPARK_HOME']+"/README.md");
#     print(text_file);
#     word_count = text_file.flatMap(lambda line:line.split()).map(lambda word:(word,1)).reduceByKey(operator.add);
#        
#     for wc in word_count.collect():
#         print wc
    data = ["a","b","c","d"];
    distributedDataset = sc.parallelize(data,4);
    local_data_taken_from_distribution = distributedDataset.getNumPartitions();
    for part in range(local_data_taken_from_distribution):
        part_rdd = distributedDataset.mapPartitionsWithIndex(make_part_filter(part),True);
        
       
       
        
    print("the number of partitions are %s"%local_data_taken_from_distribution);
   
    
    
    print(distributedDataset.reduce(lambda a,b:a+b));

