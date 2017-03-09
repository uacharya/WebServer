'''
Created on Mar 10, 2016

@author: walluser
'''

from pyspark import SparkConf,SparkContext;
import math;
import os;

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
    date_for_comparision = first["Date"].strip();
    
    # creating directory for each date
    if not os.path.exists("/Users/walluser/Desktop/dataset/" + date_for_comparision):
        os.mkdir("/Users/walluser/Desktop/dataset/" + date_for_comparision);
        # directory to hold dataset in csv file for reach node in wall display starting from 1 to 9    
        for index in range(1, 10):
            os.mkdir("/Users/walluser/Desktop/dataset/" + date_for_comparision + "/node" + str(index));
        
    for data in broadcast_variable.value:
        if data[0].strip() == date_for_comparision:
            compare_data_between(date_for_comparision, first, data[1]);
            pass;
        else:
            continue;
            
    return second;

# this function does the detailed comparing of a pair of stations using equations of physics to create wind flow simulation
def compare_data_between(date, first_station, second_station):
  
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
            temp_value = abs(temp_value);
            destination_wind_velocity = math.sqrt(temp_value);
            
            wind_flow_acceleration = get_acceleration_for_wind_flow(first_station, second_station);
            time_required_to_reach_destination_in_seconds = (destination_wind_velocity - first_station_wind_velocity) / wind_flow_acceleration[0];
            create_simulation_data(date, first_station, second_station, wind_flow_acceleration, time_required_to_reach_destination_in_seconds);
            
        else:
            # getting the final destination wind velocity using bernoulli principle
            temp_value = 2 * (((second_station_pressure / second_station_air_density) - (first_station_pressure / first_station_air_density)) + 
                                        (717 * (float(second_station["Temperature"]) - float(first_station["Temperature"]))) + 
                                        (9.8 * (float(second_station["Station_Elevation"]) - float(first_station["Station_Elevation"]))) + 
                                        + (0.5 * (second_station_wind_velocity * second_station_wind_velocity)));
            temp_value = abs(temp_value);
        
            destination_wind_velocity = math.sqrt(temp_value);
            
            wind_flow_acceleration = get_acceleration_for_wind_flow(second_station, first_station);
            time_required_to_reach_destination_in_seconds = (destination_wind_velocity - second_station_wind_velocity) / wind_flow_acceleration[0];
            create_simulation_data(date, second_station, first_station, wind_flow_acceleration, time_required_to_reach_destination_in_seconds);
           
    else:
        return;
    

# this function calculates the acceleration of the wind flow from one station to another based on pressure difference        
def get_acceleration_for_wind_flow(source, destination):
    distance_betweem_two_stations = calculate_distance(float(source["Station_Latitude"]), float(source["Station_Longitude"]), float(destination["Station_Latitude"]), float(destination["Station_Longitude"]));    
    average_density = (float(source["Station_Density"]) + float(destination["Station_Density"])) / 2;
    pressure_difference = float(source["Station_Pressure"]) - float(destination["Station_Pressure"]);
    # acceleration of a wind flow from source to destination
    acceleration = (1 / average_density) * (pressure_difference / distance_betweem_two_stations[1]);
    
    return (acceleration, distance_betweem_two_stations[0]); 
    
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
    

def create_simulation_data(date, source_station, destination_station, acceleration, time_to_reach):
   
    initial_wind_velocity = last_wind_velocity = float(source_station["Station_Wind_Velocity"]);
    station_id = source_station["Station_Name"] + "_to_" + destination_station["Station_Name"];
    
    total_time = int(math.ceil(time_to_reach));
    counter = 1;
    # getting all the locations that are in between source and destination wind flow
    intermediate_locations = get_intermediate_wind_locations(source_station, destination_station, acceleration[1], total_time);
    # writing first data so that first line starts from center of the source station
    write_to_csv_data(date, station_id, intermediate_locations[0], initial_wind_velocity);
    
    while counter <= total_time:
        # calculating the new velocity for each intervals in between until the wind reaches the destination
        last_wind_velocity = find_new_wind_velocity(initial_wind_velocity, acceleration[0], counter);
        # finding the wind location after coriolis deflection for each point in the route
        actual_wind_location = find_new_wind_location(last_wind_velocity, counter, intermediate_locations);
        # writing the data to the file after finding the required attributes for a particular wind flow line
        write_to_csv_data(date, station_id, actual_wind_location, last_wind_velocity);
        counter += 1;
    
    write_to_csv_data(date,station_id, intermediate_locations[len(intermediate_locations) - 1], initial_wind_velocity);
            
# this function finds the velocity value for a particular location of a wind flow based on time that the wind started to flow from the sources            
def find_new_wind_velocity(v0, a, t):
    return v0 + (a * t)
            
# this function produces all the locations that lies in the arc distance i.e. distance between two lines on earth based on great circle distance
def get_intermediate_wind_locations(source, destination, distance_in_radians, total_time):
    
    initial_wind_location = [float(source["Station_Latitude"]), float(source["Station_Longitude"])];
    final_wind_location = [float(destination["Station_Latitude"]), float(destination["Station_Longitude"])];
    # list that stores coordinates of all the locations that is supposed to be visualized as line for wind flow
    intermediate_locations = [];
    intermediate_locations.append(initial_wind_location);
    # converting every single degree value into radian for calculations
    lat1_radians = math.radians(initial_wind_location[0]);
    lat2_radians = math.radians(final_wind_location[0]);
    lon1_radians = math.radians(initial_wind_location[1]);
    lon2_radians = math.radians(final_wind_location[1]);
    
    for counter in range(1, total_time):
        # fractions along the route from source to destination based on time intervals where velocity is measured
        f = float(counter) / float(total_time);
        a = math.sin((1 - f) * distance_in_radians) / math.sin(distance_in_radians);
        b = math.sin(f * distance_in_radians) / math.sin(distance_in_radians);
        x = a * math.cos(lat1_radians) * math.cos(lon1_radians) + b * math.cos(lat2_radians) * math.cos(lon2_radians);
        y = a * math.cos(lat1_radians) * math.sin(lon1_radians) + b * math.cos(lat2_radians) * math.sin(lon2_radians);
        z = a * math.sin(lat1_radians) + b * math.sin(lat2_radians);
        
        final_lat = math.atan2(z, math.sqrt(x ** 2 + y ** 2));
        final_lon = math.atan2(y, x);
        location_coordinate = [math.degrees(final_lat), math.degrees(final_lon)]
        intermediate_locations.append(location_coordinate);
        
    intermediate_locations.append(final_wind_location);
    
    return intermediate_locations;
        

# this function gives actual location coordinate after coriolis deflection takes place
def find_new_wind_location(velocity, counter, list_of_locations):
    current_location = list_of_locations[counter];
    current_latitute = math.radians(current_location[0]);
    # total distance for one degree longitude in that latitude
    distance_for_one_degree_longitude = 111111 * math.cos(current_latitute);
    
    coriolis_acceleration = 2 * velocity * ((2 * math.pi) / 86400) * math.sin(current_latitute);
    # gives distance displaced in meters
    distance_displaced_due_to_coriolis = 0.5 * coriolis_acceleration * (counter ** 2);
    # total degrees of deflection
    total_deflection_in_degree_of_longitude = float(distance_displaced_due_to_coriolis) / float(distance_for_one_degree_longitude);
    
    new_lon = float(current_location[1]) - total_deflection_in_degree_of_longitude;
    # returning deflected new coordinate of the location
    return [current_location[0], new_lon];
    
# this function writes the data to each csv file for each node of wall display    
def write_to_csv_data(date, key, coordinates, velocity):
    which_node_does_location_belong_to = find_node_location(coordinates);
    
    if (which_node_does_location_belong_to=="Node_1"):
        file_to_write = open("/Users/walluser/Desktop/dataset/"+date+"/node1/data.csv","a+");
        file_to_write.write(date+","+key+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity)+"\n");
    elif(which_node_does_location_belong_to=="Node_2"):
        file_to_write = open("/Users/walluser/Desktop/dataset/"+date+"/node2/data.csv","a+");
        file_to_write.write(date+","+key+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity)+"\n");
    elif(which_node_does_location_belong_to=="Node_3"):
        file_to_write = open("/Users/walluser/Desktop/dataset/"+date+"/node3/data.csv","a+");
        file_to_write.write(date+","+key+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity)+"\n");
    elif(which_node_does_location_belong_to=="Node_4"):
        file_to_write = open("/Users/walluser/Desktop/dataset/"+date+"/node4/data.csv","a+");
        file_to_write.write(date+","+key+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity)+"\n");
    elif(which_node_does_location_belong_to=="Node_5"):
        file_to_write = open("/Users/walluser/Desktop/dataset/"+date+"/node5/data.csv","a+");
        file_to_write.write(date+","+key+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity)+"\n");
    elif(which_node_does_location_belong_to=="Node_6"):
        file_to_write = open("/Users/walluser/Desktop/dataset/"+date+"/node6/data.csv","a+");
        file_to_write.write(date+","+key+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity)+"\n");
    elif(which_node_does_location_belong_to=="Node_7"):
        file_to_write = open("/Users/walluser/Desktop/dataset/"+date+"/node7/data.csv","a+");
        file_to_write.write(date+","+key+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity)+"\n");
    elif(which_node_does_location_belong_to=="Node_8"):
        file_to_write = open("/Users/walluser/Desktop/dataset/"+date+"/node8/data.csv","a+");
        file_to_write.write(date+","+key+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity)+"\n");
    elif(which_node_does_location_belong_to=="Node_9"):
        file_to_write = open("/Users/walluser/Desktop/dataset/"+date+"/node9/data.csv","a+");
        file_to_write.write(date+","+key+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity)+"\n");
        
# this function returns a location where the wind line belongs to among all of the monitors
def find_node_location(coordinates):
    latitude = coordinates[0];
    longitude = coordinates[1];
    
    if((latitude <= 90 and latitude >= 30) and(longitude >= -180 and longitude <= -60)):
        return "Node_1";
    elif((latitude <= 90 and latitude >= 30) and(longitude > -60 and longitude <= 60)):
        return "Node_2";
    elif((latitude <= 90 and latitude >= 30) and(longitude > 61 and longitude <= 180)):
        return "Node_3";
    elif((latitude < 30 and latitude >= -30) and(longitude >= -180 and longitude <= -60)):
        return "Node_4";
    elif((latitude < 30 and latitude >= -30) and(longitude > -60 and longitude <= 60)):
        return "Node_5";
    elif((latitude < 30 and latitude >= -30) and(longitude > 61 and longitude <= 180)):
        return "Node_6";
    elif((latitude < -30 and latitude >= -90) and(longitude >= -180 and longitude <= -60)):
        return "Node_7";
    elif((latitude < -30 and latitude >= -90) and(longitude > -60 and longitude <= 60)):
        return "Node_8";
    elif((latitude < -30 and latitude >= -90) and(longitude > 61 and longitude <= 180)):
        return "Node_9";
    
            
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
    
    final_data_after_creating_files.count();
         
    # erasing the broadcast data set after use from everywhere
    broadcast_variable.unpersist(blocking=True); 
    
    sc.stop();

