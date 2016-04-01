'''
Created on Mar 10, 2016

@author: walluser
'''

from pyspark import SparkConf, SparkContext;
import json, operator;
import os


def make_part_filter(index):
    def part_filter(split_index, iterator):
        if split_index == index:
            for element in iterator:
                for content in element:
                    yield str(content);
    
    return part_filter;

def to_CSV(data,header):
    data = header+data;
    return "\n".join(data);

def write_data_to_file(index,data):
    file_output_path =  "/Users/Uzwal/Desktop/output"+index+".csv";
    file_write = open(file_output_path,"w");
    for line in data:
        file_write.write(line)
    
        
if __name__ == '__main__':
    
    # configure the spark environment
    sparkConf = SparkConf().setAppName("WordCount").setMaster("local[4]")
    sc = SparkContext(conf=sparkConf);
        
#     #The wordcount Spark Program
#     text_file = sc.textFile(os.environ['SPARK_HOME']+"/README.md");
#     print(text_file);
#     word_count = text_file.flatMap(lambda line:line.split()).map(lambda word:(word,1)).reduceByKey(operator.add);
#        
#     for wc in word_count.collect():
#         print wc
    data = sc.textFile("/Users/Uzwal/Desktop/SVMDataSet.csv").map(lambda data:data.split("\n")).collect();
    header_of_file = data[0];
    data_from_file = data[1:];
    distributedDataset = sc.parallelize(data_from_file, 3);
    local_data_taken_from_distribution = distributedDataset.getNumPartitions();
    index = 1;
    
    for part in range(local_data_taken_from_distribution):
        part_rdd = distributedDataset.mapPartitionsWithIndex(make_part_filter(part),True);
        data_for_one_node = part_rdd.collect();
        convert_data_to_required_file = to_CSV(data_for_one_node,header_of_file);
        print(" the data for %s node is : %s" %(part,convert_data_to_required_file));  
          
     
    print("the number of partitions are %s"%local_data_taken_from_distribution);
    
    sc.stop();

