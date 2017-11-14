from pyspark import SparkConf,SparkContext;
import math;

hbase = None #global hbase connection object

def create_required_datewise_data(line):
    """splits the line into individual dimension and creates a dictionary with key value pair with key being the date and value being the station's weather variable"""
    data = line.split("\t");
    
    key = str(data[2]).strip();
     
    data_features = {"Date":key,"Station_Id":str(data[0]), "Station_Name":str(data[1]), "Temperature":str(data[3]), "Station_Pressure":str(data[6]), "Station_Density":str(data[7]), "Station_Wind_Velocity":str(data[9]),
                       "Station_Latitude":str(data[16]), "Station_Longitude":str(data[17]), "Station_Elevation":str(data[18])};
    
    
    return (key, data_features);

def create_data_from_station_data(first, second):
    """this function creates the data analyzing the two stations in comparison"""
    global hbase; #one hdfs connection per executor
    
    if(hbase==None):
        import happybase; 
        hbase = happybase.ConnectionPool(size=1,host='cshadoop.boisestate.edu');
    
    date_for_comparision = first["Date"].strip();
    
    dataset = {'node1':[],'node2':[],'node3':[],'node4':[],'node5':[],'node6':[],'node7':[],'node8':[],'node9':[]};       
    
    for data in broadcast_variable.value[date_for_comparision]:
        compare_data_between(date_for_comparision, first, data,dataset);
    
#     from datetime import datetime;
#     with hbase.connection() as db:
#         for key in dataset:
#             if(len(dataset[key])!=0):
#                 t = db.table(key.encode()); # table connection to update specific data table
#                 with t.batch(batch_size=10000) as b:
#                     for data in dataset[key]:
#                         row = datetime.now().strftime("%H%M%S%f");
#                         b.put(row.encode(),{'dataset:Date'.encode():data[0].encode(),'dataset:ID'.encode():data[1].encode(),'dataset:Source'.encode():data[2].encode(),
#                                      'dataset:Destination'.encode():data[3].encode(),'dataset:S_Lat'.encode():data[4].encode(),'dataset:S_Lon'.encode():data[5].encode(),
#                                      'dataset:D_Lat'.encode():data[6].encode(),'dataset:D_Lon'.encode():data[7].encode(),'dataset:Wind_Lat'.encode():data[8].encode(),
#                                      'dataset:Wind_Lon'.encode():data[9].encode(),'dataset:Wind_Velocity'.encode():data[10].encode()});
#                     
    
    dataset.clear(); #clearing the dictionary
    
    # append over here after all the global variable has been made        
    return second;

def compare_data_between(date, first_station, second_station,dataset):
    """this function does the detailed comparing of a pair of stations using equations of physics to create wind flow simulation"""
    global hbase;
    with hbase.connection() as db:
        table = db.table('fChecker'.encode()); # table connection to update data table
        if first_station != second_station:
            first_station_pressure = float(first_station["Station_Pressure"]);
            second_station_pressure = float(second_station["Station_Pressure"]);
            
            first_station_wind_velocity = float(first_station["Station_Wind_Velocity"]);
            second_station_wind_velocity = float(second_station["Station_Wind_Velocity"]);
            
            first_station_air_density = float(first_station["Station_Density"]);
            second_station_air_density = float(second_station["Station_Density"]);
            
            
            if(first_station_pressure > second_station_pressure):
                source_id = first_station['Station_Id'].strip();
                destination_id = second_station['Station_Id'].strip();
                source_name = first_station["Station_Name"].replace(" ","_").replace("/","_").replace(":","_").replace(".","").replace(";","").replace(",","").replace("(","").replace(")","");
                destination_name = second_station["Station_Name"].replace(" ","_").replace("/","_").replace(":","_").replace(".","").replace(";","").replace(",","").replace("(","").replace(")","");
                ID = source_id+"t"+destination_id;
                #checking if the data for particular pair has already been written
                data=None;
                data = table.row(ID.encode(),columns=[('f:'+date).encode()]);

                if(data):
                    return;
                else:
                    table.put(ID.encode(),{('f:'+date).encode():'y'.encode()});
#                 filt = "SingleColumnValueFilter('flow','"+date+"',=,'binaryprefix:"+ID+"',true,true)";
#                 res_generator = table.scan(filter=filt,limit=1);
#                 for d in res_generator:
#                     data = d;              
#                 if(data):
#                     return;
#                 else:
#                     from datetime import datetime;
#                     row = datetime.now().strftime("%H%M%S%f");
#                     table.put(row.encode(),{('flow:'+date).encode():ID.encode()}); 
                
                # getting the final destination wind velocity using bernoulli principle
                temp_value = 2 * (((first_station_pressure / first_station_air_density) - (second_station_pressure / second_station_air_density)) + 
                                            (717 * (float(first_station["Temperature"]) - float(second_station["Temperature"]))) + 
                                            (9.8 * (float(first_station["Station_Elevation"]) - float(second_station["Station_Elevation"]))) + 
                                            + (0.5 * (first_station_wind_velocity * first_station_wind_velocity)));
                destination_wind_velocity = math.sqrt(abs(temp_value));
                
                wind_flow_acceleration = get_acceleration_for_wind_flow(first_station, second_station);
                
                if(wind_flow_acceleration==None):
                    return;
                
                time_required_to_reach_destination_in_seconds = (destination_wind_velocity - first_station_wind_velocity) / wind_flow_acceleration[0];
                create_simulation_data(date,ID,source_name,destination_name, first_station, second_station, wind_flow_acceleration, time_required_to_reach_destination_in_seconds,destination_wind_velocity,dataset);
                
            elif second_station_pressure>first_station_pressure:
                source_id= second_station['Station_Id'].strip();
                destination_id = first_station['Station_Id'].strip();
                source_name = second_station["Station_Name"].replace(" ","_").replace("/","_").replace(":","_").replace(".","").replace(";","").replace(",","").replace("(","").replace(")","");
                destination_name = first_station["Station_Name"].replace(" ","_").replace("/","_").replace(":","_").replace(".","").replace(";","").replace(",","").replace("(","").replace(")","");
                ID = source_id+"t"+destination_id;
                #checking if the data for particular pair has already been written
                data=None;
                data = table.row(ID.encode(),columns=[('f:'+date).encode()]);

                if(data):
                    return;
                else:
                    table.put(ID.encode(),{('f:'+date).encode():'y'.encode()});

#                 filt = "SingleColumnValueFilter('flow','"+date+"',=,'binaryprefix:"+ID+"',true,true)";
#                 res_generator = table.scan(filter=filt,limit=1);
#                 for d in res_generator:
#                     data = d;              
#                 if(data):
#                     return;
#                 else:
#                     from datetime import datetime;
#                     row = datetime.now().strftime("%H%M%S%f");
#                     table.put(row.encode(),{('flow:'+date).encode():ID.encode()}); 
    
                # getting the final destination wind velocity using bernoulli principle
                temp_value = 2 * (((second_station_pressure / second_station_air_density) - (first_station_pressure / first_station_air_density)) + 
                                            (717 * (float(second_station["Temperature"]) - float(first_station["Temperature"]))) + 
                                            (9.8 * (float(second_station["Station_Elevation"]) - float(first_station["Station_Elevation"]))) + 
                                            + (0.5 * (second_station_wind_velocity * second_station_wind_velocity)));
                                                                        
                destination_wind_velocity = math.sqrt(abs(temp_value));
                
                wind_flow_acceleration = get_acceleration_for_wind_flow(second_station, first_station);
                
                if(wind_flow_acceleration==None):
                    return;
                
                time_required_to_reach_destination_in_seconds = (destination_wind_velocity - second_station_wind_velocity) / wind_flow_acceleration[0];
                create_simulation_data(date,ID,source_name,destination_name, second_station, first_station, wind_flow_acceleration, time_required_to_reach_destination_in_seconds,destination_wind_velocity,dataset);
               
        else:
            return;
                  
    

# this function calculates the acceleration of the wind flow from one station to another based on pressure difference        
def get_acceleration_for_wind_flow(source, destination):
    """this function calculates the acceleration of the wind flow from one station to another based on pressure difference"""
    distance_betweem_two_stations = calculate_distance(float(source["Station_Latitude"]), float(source["Station_Longitude"]), float(destination["Station_Latitude"]), float(destination["Station_Longitude"]));    
    average_density = (float(source["Station_Density"]) + float(destination["Station_Density"])) / 2;
    pressure_difference = float(source["Station_Pressure"]) - float(destination["Station_Pressure"]);
    
    if(distance_betweem_two_stations[1]==0.0):
        return None;
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
    

def create_simulation_data(date,ID,source_name,destination_name, source_station, destination_station, acceleration, time_to_reach,final_velocity,dataset):
    """This function writes the data into each node data holder by performing a bit of calculation first"""
    start_velocity = initial_wind_velocity = float(source_station["Station_Wind_Velocity"]);
    total_time = int(math.ceil(acceleration[1] * 3959 )); #distance in miles between points
    time_in_seconds_per_step = time_to_reach/float(total_time);
    counter = 1;
    # getting all the locations that are in between source and destination wind flow
    intermediate_locations = get_intermediate_wind_locations(source_station, destination_station, acceleration[1], total_time);
    # writing first data so that first line starts from center of the source station
    write_to_csv_data(date,ID,source_name,destination_name,source_station["Station_Latitude"], source_station["Station_Longitude"], destination_station["Station_Latitude"],destination_station["Station_Longitude"],intermediate_locations[0],start_velocity,dataset);
    
    while counter <= total_time:
        # finding the wind location after coriolis deflection for each point in the route
        actual_wind_location = find_new_wind_location(initial_wind_velocity, counter, intermediate_locations);
        # calculating the new velocity for each intervals in between until the wind reaches the destination
        last_wind_velocity = find_new_wind_velocity(start_velocity, acceleration[0], (counter*time_in_seconds_per_step));
        # writing the data to the file after finding the required attributes for a particular wind flow line
        write_to_csv_data(date,ID, source_name,destination_name,source_station["Station_Latitude"],source_station["Station_Longitude"],destination_station["Station_Latitude"],destination_station["Station_Longitude"], actual_wind_location, last_wind_velocity,dataset);
        
        initial_wind_velocity = last_wind_velocity;
        counter += 1;
    
#     write_to_csv_data(date,ID,source_id,destination_id,source_station["Station_Latitude"],source_station["Station_Longitude"],destination_station["Station_Latitude"],destination_station["Station_Longitude"], intermediate_locations[len(intermediate_locations) - 1], final_velocity);
                 
       
def find_new_wind_velocity(v0, a, t): 
    """this function finds the velocity value for a particular location of a wind flow based on time that the wind started to flow from the sources"""
    return v0 + (a * t)
            
def get_intermediate_wind_locations(source, destination, distance_in_radians, total_time):
    """this function produces all the locations that lies in the arc distance i.e. distance between two lines on earth based on great circle distance"""
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
        

def find_new_wind_location(velocity, counter, list_of_locations):
    """this function gives actual location coordinate after coriolis deflection takes place"""
    current_location = list_of_locations[counter];
    current_latitute = math.radians(current_location[0]);
    start_latitude = math.radians(list_of_locations[counter-1][0]);
    TOF = 960;
    # total distance for one degree longitude in that latitude
    distance_for_one_degree_longitude = 111111 * math.cos(current_latitute);
    
    coriolis_acceleration = 2 * velocity * ((2 * math.pi) / 86400) * math.sin(start_latitude);
    # gives distance displaced in meters
    distance_displaced_due_to_coriolis = 0.5 * coriolis_acceleration * (TOF ** 2);
    # total degrees of deflection
    total_deflection_in_degree_of_longitude = float(distance_displaced_due_to_coriolis) / float(distance_for_one_degree_longitude);
    
    new_lon = float(current_location[1]) - total_deflection_in_degree_of_longitude;
    # returning deflected new coordinate of the location
    return (current_location[0], new_lon);
    
# this function writes the data to each csv file for each node of wall display    
def write_to_csv_data(date,ID, source_id,destination_id,source_lat,source_lon,destination_lat,destination_lon, coordinates, velocity,dataset):
    """This function writes data for a particular stream flow into every single node data holder list for writing them later to hdfs""" 
    which_node_does_location_belong_to = find_node_location(coordinates);
    content = [date,ID,source_id,destination_id,source_lat,source_lon,destination_lat,destination_lon,str(coordinates[0]),str(coordinates[1]),str(velocity)];
  
    if (which_node_does_location_belong_to=="Node_1"):
        dataset["node1"].append(content);
    elif(which_node_does_location_belong_to=="Node_2"):
        dataset["node2"].append(content);
    elif(which_node_does_location_belong_to=="Node_3"):
        dataset["node3"].append(content);
    elif(which_node_does_location_belong_to=="Node_4"):
        dataset["node4"].append(content);
    elif(which_node_does_location_belong_to=="Node_5"):
        dataset["node5"].append(content);
    elif(which_node_does_location_belong_to=="Node_6"):
        dataset["node6"].append(content);
    elif(which_node_does_location_belong_to=="Node_7"):
        dataset["node7"].append(content);
    elif(which_node_does_location_belong_to=="Node_8"):
        dataset["node8"].append(content);
    elif(which_node_does_location_belong_to=="Node_9"):
        dataset["node9"].append(content);    
                     
def find_node_location(coordinates):
    """this function returns a location where the wind line belongs to among all of the monitors based on mercator projection"""
    latitude = coordinates[0];
    longitude = coordinates[1];
    
    if((latitude <= 79 and latitude >= 54.548) and(longitude >= -180 and longitude <= -60.021)):
        return "Node_1";
    elif((latitude <= 79 and latitude >= 54.548) and(longitude >=-60 and longitude <= 59.989)):
        return "Node_2";
    elif((latitude <= 79 and latitude >= 54.548) and(longitude >=60 and longitude <= 180)):
        return "Node_3";
    elif((latitude <=54.52 and latitude >= -2.155) and(longitude >= -180 and longitude <= -60.021)):
        return "Node_4";
    elif((latitude <=54.52 and latitude >= -2.155) and(longitude >= -60 and longitude <= 59.989)):
        return "Node_5";
    elif((latitude <=54.52 and latitude >= -2.155) and(longitude >= 60 and longitude <= 180)):
        return "Node_6";
    elif((latitude <=-2.187 and latitude >= -56.97) and(longitude >= -180 and longitude <= -60.021)):
        return "Node_7";
    elif((latitude <=-2.187 and latitude >= -56.97) and(longitude >= -60 and longitude <= 59.989)):
        return "Node_8";
    elif((latitude <=-2.187 and latitude >= -56.97) and(longitude >= 60 and longitude <= 180)):
        return "Node_9";
    
            
if __name__ == '__main__':
    
    import happybase;
    from collections import defaultdict;
    # configure the spark environment
    sparkConf = SparkConf().setAppName("Simulating Streamline");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sc = SparkContext(conf=sparkConf);

    distributed_dataset = sc.textFile("hdfs:/user/uacharya/subset_small_dataset.txt",minPartitions=45);
    print("this is the driver container");
    # getting the header of the whole dataset
    header = distributed_dataset.first();
    # filtering the header out of the data 
    distributed_dataset = distributed_dataset.filter(lambda d: d != header);
    # mapping the data to prepare for processing
    data_in_required_format = distributed_dataset.map(create_required_datewise_data);
    data_in_required_format.cache();
    #collecting all the dataset for broadcasting
    broadcast_data = data_in_required_format.collect();
    print(str(len(broadcast_data))+" driver program");
    broadcast_data_based_on_id = defaultdict(list);  # for holding nested data for streamline based on flow ID between two stations
    
    for line in broadcast_data:
        broadcast_data_based_on_id[line[0].strip()].append(line[1]);
    #broadcasting the entire dataset  
    broadcast_variable = sc.broadcast(broadcast_data_based_on_id);
    
    database = happybase.ConnectionPool(size=1,host='cshadoop.boisestate.edu');
    #getting a connection from the pool
    with database.connection() as db:
        for index in range(1,10):
            index = 'node'+str(index);
            db.create_table(index.encode(), {
                'dataset'.encode():dict()
                });
                
        db.create_table('fChecker'.encode(),{'f'.encode():dict(max_versions=1,in_memory=True)});
    
    temp = set(data_in_required_format.keys().collect());
    #getting keys for use in future
    sorted_keys = sorted(temp,key=int);
    #writing the keys value to a file
#     hdfs = PyWebHdfsClient(host='cshadoop.boisestate.edu',port='50070', user_name='uacharya');
#     keys_data = str(sorted_keys);
#     hdfs.create_file('user/uacharya/keys.txt',keys_data);         
    # analyzing the stations weather variables based on each date to create the simulation data for wind flow      
    final_data_after_creating_files = data_in_required_format.reduceByKey(create_data_from_station_data);
    
    x = final_data_after_creating_files.count();   
    # erasing the broadcast data set after use from everywhere
    broadcast_variable.unpersist(blocking=True); 
    
    sc.stop();

