'''
Created on Mar 10, 2016

@author: walluser
'''

from pyspark import SparkConf, SparkContext;
import os;
import math;

# splits the line into individual dimension and creates a dictionary with key value pair 
# with key being the date and value being the station's weather variable
def create_required_datewise_data(line):
    
    data = line.split("\t");
    
    key = str(data[1]);
    
    data_features = {"Date":key, "Station_Name":str(data[0]), "Temperature":str(data[2]), "Station_Pressure":str(data[5]), "Station_Density":str(data[6]), "Station_Wind_Velocity":str(data[8]),
                       "Station_Latitude":str(data[15]), "Station_Longitude":str(data[16]), "Station_Elevation":str(data[17])};
    
    
    return (key, data_features);

# this function creates the data analyzing the two stations in comparison
def create_data_from_station_data(first, second):
    date_for_comparision = first["Date"];
    
    for data in broadcast_variable.value:
        if data[0] == date_for_comparision:
            compare_data_between(first, data[1]);
        else:
            continue;
            
    return second;


def compare_data_between(first_station, second_station):
  
    if first_station != second_station:
        first_station_pressure = float(first_station["Station_Pressure"]);
        second_station_pressure = float(second_station["Station_Pressure"]);
        
        first_station_wind_velocity = float(first_station["Station_Wind_Velocity"]);
        second_station_wind_velocity = float(second_station["Station_Wind_Velocity"]);
        
        destination_wind_velocity = 0;
        
        first_station_air_density = float(first_station["Station_Density"]);
        second_station_air_density = float(second_station["Station_Density"]);
        
        if(first_station_pressure > second_station_pressure):
            temp_value = 2 * (((first_station_pressure / first_station_air_density) - (second_station_pressure / second_station_air_density)) + 
                                        (717 * (float(first_station["Temperature"]) - float(second_station["Temperature"]))) + 
                                        (9.8 * (float(first_station["Station_Elevation"]) - float(second_station["Station_Elevation"]))) + 
                                        + (0.5 * (first_station_wind_velocity * first_station_wind_velocity)));
        
            
            destination_wind_velocity = math.sqrt(temp_value);
            
            wind_flow_acceleration = get_acceleration_for_wind_flow(first_station, second_station);
            time_required_to_reach_destination_in_seconds = (destination_wind_velocity - first_station_wind_velocity) / wind_flow_acceleration;
            create_simulation_data(first_station,second_station,wind_flow_acceleration,time_required_to_reach_destination_in_seconds);
        else:
            temp_value = 2 * (((second_station_pressure / second_station_air_density) - (first_station_pressure / first_station_air_density)) + 
                                        (717 * (float(second_station["Temperature"]) - float(first_station["Temperature"]))) + 
                                        (9.8 * (float(second_station["Station_Elevation"]) - float(first_station["Station_Elevation"]))) + 
                                        + (0.5 * (second_station_wind_velocity * second_station_wind_velocity)));
        
            
            destination_wind_velocity = math.sqrt(temp_value);
            wind_flow_acceleration = get_acceleration_for_wind_flow(second_station, first_station);
            time_required_to_reach_destination_in_seconds = (destination_wind_velocity - second_station_wind_velocity) / wind_flow_acceleration;
            create_simulation_data(second_station,first_station,wind_flow_acceleration,time_required_to_reach_destination_in_seconds);
           
    else:
        return;
    
def create_simulation_data(source_station,destination_station,acceleration,time_to_reach):
    pass;


# this function calculates the acceleration of the wind flow from one station to another based on pressure difference        
def get_acceleration_for_wind_flow(source, destination):
    distance_betweem_two_stations = calculate_distance(float(source["Station_Latitude"]), float(source["Station_Longitude"]), float(destination["Station_Latitude"]), float(destination["Station_Longitude"]));    
    average_density = (float(source["Station_Density"]) + float(destination["Station_Density"])) / 2;
    pressure_difference = float(source["Station_Pressure"]) - float(destination["Station_Pressure"]);
    # acceleration of a wind flow from source to destination
    acceleration = (1 / average_density) * (pressure_difference / distance_betweem_two_stations);
    
    return acceleration; 
    
# this function calculates distance between two points in earth based on their lat and lon
def calculate_distance(lat1, lon1, lat2, lon2):
    rad_lat1 = math.pi * lat1 / 180;
    rad_lat2 = math.pi * lat2 / 180;
    rad_theta = math.pi * (lon1 - lon2) / 180;
    distance = math.sin(rad_lat1) * math.sin(rad_lat2) + math.cos(rad_lat1) * math.cos(rad_lat2) * math.cos(rad_theta);
    distance = math.acos(distance) * (180 / math.pi);
    # distance in kilometers
    final_distance = distance * 60 * 1.1515 * 1.609344;
    
    return final_distance * 1000;
    
    
    
            
if __name__ == '__main__':
    
    # configure the spark environment
    sparkConf = SparkConf().setAppName("Creating Data").setMaster("local[*]");
    sc = SparkContext(conf=sparkConf);
    
    distributed_dataset = sc.textFile("file:///Users/walluser/Desktop/preprocessed_combined.txt");
    # getting the header of the whole dataset
    header = distributed_dataset.first();
    # filtering the header out of the data 
    distributed_dataset = distributed_dataset.filter(lambda d: d != header);
    # mapping the data to prepare for processing
    data_in_required_format = distributed_dataset.map(create_required_datewise_data);
  
    broadcast_data = data_in_required_format.collect();
      
    broadcast_variable = sc.broadcast(broadcast_data);
    
        
    # analyzing the stations weather variables based on each date to create the simulation data for wind flow      
    final_data_after_creating_files = data_in_required_format.reduceByKey(create_data_from_station_data);
         
    # erasing the broadcast data set after use from everywhere
    broadcast_variable.unpersist(blocking=True); 
    
    sc.stop();

