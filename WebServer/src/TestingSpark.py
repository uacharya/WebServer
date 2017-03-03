'''
Created on Mar 10, 2016

@author: walluser
'''

from pyspark import SparkConf, SparkContext;

#splits the line into individual dimension and creates a dictionary with key value pair 
#with key being the date and value being the station's weather variable
def create_required_datewise_data(line):
    
    data = line.split("\t");
    
    data_features = {"Station_Name":str(data[0]), "Temperature":str(data[2]), "Station_Pressure":str(data[5]), "Station_Density":str(data[6]), "Station_Wind_Velocity":str(data[8]),
                       "Station_Latitude":str(data[15]), "Station_Longitude":str(data[16]), "Station_Elevation":str(data[17])};
    key = str(data[1]);
    
    return (key, data_features);

#this function creates the data analyzing the two stations in comparison
def create_data_from_station_data(first,second):
    
    
    return second;
    
            
if __name__ == '__main__':
    
    # configure the spark environment
    sparkConf = SparkConf().setAppName("Creating Data").setMaster("local[*]");
    sc = SparkContext(conf=sparkConf);
    
    distributed_dataset = sc.textFile("file:///Users/walluser/Desktop/preprocessed_combined.txt");
    #getting the header of the whole dataset
    header = distributed_dataset.first();
    #filtering the header out of the data 
    distributed_dataset = distributed_dataset.filter(lambda d: d!=header);
    #mapping the data to prepare for processing
    data_in_required_format = distributed_dataset.map(create_required_datewise_data);
  
    broadcast_data = data_in_required_format.collect();
      
    broadcast_variable = sc.broadcast(broadcast_data);
    
    #analyzing the stations weather variables based on each date to create the simulation data for wind flow      
    final_data_after_creating_files = data_in_required_format.reduceByKey(create_data_from_station_data);
         
    #erasing the broadcast data set after use from everywhere
    broadcast_variable.unpersist(blocking=True); 
    
    sc.stop();

