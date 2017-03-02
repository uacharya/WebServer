'''
Created on Mar 10, 2016

@author: walluser
'''

from pyspark import SparkConf, SparkContext;


def create_required_datewise_data(line):
    
    data = line.split("\t");
    
    data_dictionary = {"Station":str(data[0]), "temperature":str(data[2]), "pressure":str(data[5]), "Density":str(data[6]), "Wind_Velocity":str(data[8]),
                       "Latitude":str(data[15]), "Longitude":str(data[16]), "Elevation":str(data[17])};
    key = str(data[1]);
    
    return (key, data_dictionary);

def collecting_data(split_index, iterator):
    if(split_index == 0):
        for data in iterator:
            yield str(data);
    
            
if __name__ == '__main__':
    
    # configure the spark environment
    sparkConf = SparkConf().setAppName("Creating Data").setMaster("local[4]");
    sc = SparkContext(conf=sparkConf);
    
    distributed_dataset = sc.textFile("/Users/Uzwal/Desktop/preprocessed_combined.txt");
    #getting the header of the whole dataset
    header = distributed_dataset.first();
      
    distributed_dataset = distributed_dataset.filter(lambda d: d!=header);
     
    data_in_required_format = distributed_dataset.map(create_required_datewise_data);
  
    broadcast_data = data_in_required_format.collect();
      
    broadcast_variable = sc.broadcast(broadcast_data);
    
    output = data_in_required_format.collect();
     
#     for item in output:
#         print(item);
     
      
    print("the keys are" + str(data_in_required_format.getNumPartitions()));
     
    print(broadcast_variable.value);
    
    #erasing the broadcast data set after use from everywhere
    broadcast_variable.unpersist(blocking=True); 
    
    sc.stop();

