'''
Created on Mar 10, 2016

@author: walluser
'''

from pyspark import SparkConf, SparkContext;
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
        
        first_station_air_density = float(first_station["Station_Density"]);
        second_station_air_density = float(second_station["Station_Density"]);
        
        if(first_station_pressure > second_station_pressure):
            # getting the final destination wind velocity using bernoulli principle
            temp_value = 2 * (((first_station_pressure / first_station_air_density) - (second_station_pressure / second_station_air_density)) + 
                                        (717 * (float(first_station["Temperature"]) - float(second_station["Temperature"]))) + 
                                        (9.8 * (float(first_station["Station_Elevation"]) - float(second_station["Station_Elevation"]))) + 
                                        + (0.5 * (first_station_wind_velocity * first_station_wind_velocity)));
        
            
            destination_wind_velocity = math.sqrt(temp_value);
            
            wind_flow_acceleration = get_acceleration_for_wind_flow(first_station, second_station);
            time_required_to_reach_destination_in_seconds = (destination_wind_velocity - first_station_wind_velocity) / wind_flow_acceleration[0];
            create_simulation_data(first_station, second_station, wind_flow_acceleration, time_required_to_reach_destination_in_seconds);
            
        else:
            # getting the final destination wind velocity using bernoulli principle
            temp_value = 2 * (((second_station_pressure / second_station_air_density) - (first_station_pressure / first_station_air_density)) + 
                                        (717 * (float(second_station["Temperature"]) - float(first_station["Temperature"]))) + 
                                        (9.8 * (float(second_station["Station_Elevation"]) - float(first_station["Station_Elevation"]))) + 
                                        + (0.5 * (second_station_wind_velocity * second_station_wind_velocity)));
        
            
            destination_wind_velocity = math.sqrt(temp_value);
            
            wind_flow_acceleration = get_acceleration_for_wind_flow(second_station, first_station);
            time_required_to_reach_destination_in_seconds = (destination_wind_velocity - second_station_wind_velocity) / wind_flow_acceleration[0];
            create_simulation_data(second_station, first_station, wind_flow_acceleration, time_required_to_reach_destination_in_seconds);
           
    else:
        return;
    

# this function calculates the acceleration of the wind flow from one station to another based on pressure difference        
def get_acceleration_for_wind_flow(source, destination):
    distance_betweem_two_stations = calculate_distance(float(source["Station_Latitude"]), float(source["Station_Longitude"]), float(destination["Station_Latitude"]), float(destination["Station_Longitude"]));    
    average_density = (float(source["Station_Density"]) + float(destination["Station_Density"])) / 2;
    pressure_difference = float(source["Station_Pressure"]) - float(destination["Station_Pressure"]);
    # acceleration of a wind flow from source to destination
    acceleration = (1 / average_density) * (pressure_difference / distance_betweem_two_stations[1]);
    
    return (acceleration,distance_betweem_two_stations[0]); 
    
# this function calculates distance between two points in earth based on their lat and lon using great circle formula os sphere
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371000;  # meters the radius of earth
    rad_lat1 = math.pi * lat1 / 180;
    rad_lat2 = math.pi * lat2 / 180;
    rad_diff_lat = math.pi * (lat2 - lat1) / 180;
    rad_diff_lon = math.pi * (lon2 - lon1) / 180;
    a = math.sin(rad_diff_lat / 2) ** 2 + math.cos(rad_lat1) * math.cos(rad_lat2) * (math.sin(rad_diff_lon / 2) ** 2);
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a));
    # distance between the two points in meters
    return (c, R * c);
    

def create_simulation_data(source_station, destination_station, acceleration, time_to_reach):
   
    initial_wind_velocity = last_wind_velocity = float(source_station["Station_Wind_Velocity"]);
    station_id = source_station["Station_Name"];
    
    total_time = math.ceil(time_to_reach);
    counter = 0;
    #getting all the locations that are in between source and destination wind flow
    intermediate_locations = get_intermediate_wind_locations(source_station, destination_station,acceleration[1],total_time);
    # writing first data so that first line starts from center of the source station
    write_to_csv_data(station_id,intermediate_locations[0],initial_wind_velocity);
    
    while counter <= total_time:
        # finding the wind location after coriolis deflection for each point in the route
        actual_wind_location = find_new_wind_location(last_wind_velocity, counter,intermediate_locations);
        # writing the data to the file after finding the required attributes for a particular wind flow line
        write_to_csv_data(station_id, actual_wind_location, last_wind_velocity);
        counter += 1;
        # calculating the new velocity for each intervals in between until the wind reaches the destination
        if(counter <= total_time): 
            last_wind_velocity = find_new_wind_velocity(initial_wind_velocity, acceleration[0], counter);
            
#this function finds the velocity value for a particular location of a wind flow based on time that the wind started to flow from the sources            
def find_new_wind_velocity(v0, a, t):
    return v0 + (a * t)
            
#this function produces all the locations that lies in the arc distance i.e. distance between two lines on earth based on great circle distance
def get_intermediate_wind_locations(source,destination,distance_in_radians,total_time):
    
    initial_wind_location = [float(source["Station_Latitude"]), float(source["Station_Longitude"])];
    final_wind_location =[float(destination["Station_Latitude"]), float(destination["Station_Longitude"])];
    #list that stores coordinates of all the locations that is supposed to be visualized as line for wind flow
    intermediate_locations=[];
    intermediate_locations.append(initial_wind_location);
    #converting every single degree value into radian for calculations
    lat1_radians = math.radians(initial_wind_location[0]);
    lat2_radians = math.radians(final_wind_location[0]);
    lon1_radians = math.radians(initial_wind_location[1]);
    lon2_radians = math.radians(final_wind_location[1]);
    
    for counter in range(1,total_time):
        #fractions along the route from source to destination based on time intervals where velocity is measured
        f = float(counter)/float(total_time);
        a = math.sin((1-f)*distance_in_radians)/math.sin(distance_in_radians);
        b = math.sin(f*distance_in_radians)/math.sin(distance_in_radians);
        x = a*math.cos(lat1_radians)*math.cos(lon1_radians)+b*math.cos(lat2_radians)*math.cos(lon2_radians);
        y = a*math.cos(lat1_radians)*math.sin(lon1_radians)+b*math.cos(lat2_radians)*math.sin(lon2_radians);
        z = a*math.sin(lat1_radians)+b*math.sin(lat2_radians);
        
        final_lat = math.atan2(z, math.sqrt(x**2+y**2));
        final_lon = math.atan2(y, x);
        location_coordinate = [math.degrees(final_lat),math.degrees(final_lon)]
        intermediate_locations.append(location_coordinate);
        
    intermediate_locations.append(final_wind_location);
    
    return intermediate_locations;
        

#this function gives actual location coordinate after coriolis deflection takes place
def find_new_wind_location(velocity, counter,list_of_locations):
    pass;







def write_to_csv_data(key, coordinates, velocity):
    pass;
            
if __name__ == '__main__':
    
    # configure the spark environment
    sparkConf = SparkConf().setAppName("Creating Data");
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

