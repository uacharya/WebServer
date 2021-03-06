'''
Created on Apr 9, 2018

@author: Ujjwal Acharya
'''

#!/usr/bin/python2.7
from pyspark import SparkConf,SparkContext;
import math,time;
from pyspark.serializers import MarshalSerializer;

hdfs = None #global hdfs connection object one for each thread;
hbase = None #global hbase connection object one for each thread;

def create_required_datewise_data(line):
    """splits the line into individual dimension and creates a dictionary with key value pair with key being the date and value being the station's weather variable"""
    data = line.split("\t");
    
    key = str(data[2]).strip();
     
    data_features = {"Date":key,"Station_Id":str(data[0]), "Station_Name":str(data[1]), "Temperature":str(data[3]), "Station_Pressure":str(data[6]), "Station_Density":str(data[7]), "Station_Wind_Velocity":str(data[9]),
                       "Station_Latitude":str(data[16]), "Station_Longitude":str(data[17]), "Station_Elevation":str(data[18])};
    
    
    return (key,data_features);

def create_data_from_station_data(first, second):
    """this function creates the data analyzing the two stations in comparison"""
    global hdfs; #global hdfs object
    global hbase; #global hbase object
    
    if(hdfs is None): 
        from pywebhdfs.webhdfs import PyWebHdfsClient; 
        hdfs = PyWebHdfsClient(host='cshadoop.boisestate.edu',port='50070', user_name='uacharya'); 
   
    if(hbase is None):
        import happybase;
        hbase = happybase.ConnectionPool(size=1,host='cshadoop.boisestate.edu');
 
    date_for_comparision = first["Date"].strip();

    # creating directory for each date
    try:
        hdfs.get_file_dir_status('user/uacharya/single_screen/'+date_for_comparision);
    except Exception:
        # directory to hold dataset in csv file for reach node in wall display starting from 1 to 9    
        content = 'Date,ID,Source,Destination,S_Lat,S_Lon,D_Lat,D_Lon,Wind_Lat,Wind_Lon,Wind_Velocity\n';
        try:
            hdfs.create_file('user/uacharya/single_screen/'+date_for_comparision+'/data/output.csv',content,replication=1);
        except Exception:
            pass
   
    
    dataset = {'node_1':[],'node_2':[],'node_3':[]};
   
    for data in broadcast_variable.value:
        compare_data_between(date_for_comparision, first, data,dataset);

#    for key in dataset:
#        if(len(dataset[key])!=0):
#            content = "\n".join(dataset[key]);
#            content +="\n";
#            while(True):
#                try:
#                    hdfs.append_file('user/uacharya/simulation/'+date+'/'+key+'/output.csv',content,buffersize=4096);
#                    break;
#                except Exception:
#                    time.sleep(0.2);
#                    continue;

    
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
                source_name = first_station["Station_Name"].replace(" ","_").replace("/","_").replace(":","_").replace(".","").replace(";","").replace(",","").replace("(","").replace(")","").replace("&","").replace("'","").replace("?","").replace("#","").replace("=","").replace("*","").replace("`","")
                destination_name = second_station["Station_Name"].replace(" ","_").replace("/","_").replace(":","_").replace(".","").replace(";","").replace(",","").replace("(","").replace(")","").replace("&","").replace("'","").replace("?","").replace("#","").replace("=","").replace("*","").replace("`","")
                ID = source_id+"t"+destination_id;
                #checking if the data for particular pair has already been written
                data=None;
                data = table.row(ID.encode(),columns=[('f:'+date).encode()]);

                if(data):
                    return;
                else:
                    table.put(ID.encode(),{('f:'+date).encode():'y'.encode()});
                
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
                source_name = second_station["Station_Name"].replace(" ","_").replace("/","_").replace(":","_").replace(".","").replace(";","").replace(",","").replace("(","").replace(")","").replace("&","").replace("'","").replace("?","").replace("#","").replace("=","").replace("*","").replace("`","");
                destination_name = first_station["Station_Name"].replace(" ","_").replace("/","_").replace(":","_").replace(".","").replace(";","").replace(",","").replace("(","").replace(")","").replace("&","").replace("'","").replace("?","").replace("#","").replace("=","").replace("*","").replace("`","");
                ID = source_id+"t"+destination_id;
                #checking if the data for particular pair has already been written
                data=None;
                data = table.row(ID.encode(),columns=[('f:'+date).encode()]);

                if(data):
                    return;
                else:
                    table.put(ID.encode(),{('f:'+date).encode():'y'.encode()});

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
    

def calculate_distance(lat1, lon1, lat2, lon2):
    """this function calculates distance between two points in earth based on their lat and lon using great circle formula os sphere"""
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
#     total_distance = math.ceil(acceleration[1] * 3959 ); #distance in miles between points
    #larger distance between two points either lon diff or lat diff
    diff = find_larger_diff_between(float(source_station["Station_Latitude"]),float(source_station["Station_Longitude"]),float(destination_station["Station_Latitude"]),float(destination_station["Station_Longitude"]));
    #finding interpolation points between two stations to a reasonable steps
    total_steps = int(math.ceil(diff));
    time_in_seconds_per_step = float(time_to_reach)/float(total_steps);
    counter = 1;
    # getting all the locations that are in between source and destination wind flow
    intermediate_locations = get_intermediate_wind_locations(source_station, destination_station, acceleration[1], total_steps);
    # writing first data so that first line starts from center of the source station
    write_to_csv_data(date,ID,source_name,destination_name,source_station["Station_Latitude"], source_station["Station_Longitude"], destination_station["Station_Latitude"],destination_station["Station_Longitude"],intermediate_locations[0],start_velocity,dataset);
    
    while counter <= total_steps:
        # finding the wind location after coriolis deflection for each point in the route
        #actual_wind_location = find_new_wind_location(initial_wind_velocity, counter,time_in_seconds_per_step, intermediate_locations);
        actual_wind_location = intermediate_locations[counter]  
    # calculating the new velocity for each intervals in between until the wind reaches the destination
        last_wind_velocity = find_new_wind_velocity(start_velocity, acceleration[0], (counter*time_in_seconds_per_step));
        # writing the data to the file after finding the required attributes for a particular wind flow line
        write_to_csv_data(date,ID, source_name,destination_name,source_station["Station_Latitude"],source_station["Station_Longitude"],destination_station["Station_Latitude"],destination_station["Station_Longitude"], actual_wind_location, last_wind_velocity,dataset);
        
        initial_wind_velocity = last_wind_velocity;
        counter += 1;
#     write_to_csv_data(date,ID,source_id,destination_id,source_station["Station_Latitude"],source_station["Station_Longitude"],destination_station["Station_Latitude"],destination_station["Station_Longitude"], intermediate_locations[len(intermediate_locations) - 1], final_velocity);
    write_to_hdfs_for_one_flow(date,dataset);

 
def find_total_wind_flow_points(coordinates_diff):
    """This functions determines how many points should be plotted between two points based on the geographical locations of source and destination stations"""
    coordinates_diff = math.ceil(coordinates_diff);
    
    if coordinates_diff <=1:
        total_distance= 5;
    elif coordinates_diff<5:
        step = 5-int(coordinates_diff);
        mul = 2+(0.1*step);
        total_distance= int(math.ceil(coordinates_diff*mul));
    elif coordinates_diff<=50:
        rang =  coordinates_diff/10;
        step = 2 - (0.1*math.floor(rang));
        total_distance = int(math.ceil(coordinates_diff*step));
    else:
        quotient = math.ceil(coordinates_diff/100);
        total_distance= int(quotient*100);
          
    return total_distance;
 
        
def find_larger_diff_between(lat1,lon1,lat2,lon2):
    """This function finds the difference in degrees between two stations and returns the largest difference between longitude difference or latitude difference"""
    lat_diff = lat1-lat2 if lat1>=lat2 else lat2-lat1;
    lon_diff = lon1-lon2 if lon1>=lon2 else lon2-lon1;
    if(lon_diff>=lat_diff):
        return lon_diff;
    else:
        return lat_diff

def write_to_hdfs_for_one_flow(date,dataset):
    """This function takes the dataset dictionary that holds the data and writes once for each flow so that the dictionary doesnot have to hold a lot of data
    before finally appending to hdfs which may use a lot of memory"""
    global hdfs;
    
    for key in dataset:
        if(len(dataset[key])!=0):
            content = "\n".join(dataset[key]);
            content +="\n";
            while(True):
                try:
                    hdfs.append_file('user/uacharya/single_screen/'+date+'/data/output.csv',content,buffersize=4096);
                    break;
                except Exception:
                    time.sleep(0.3);
                    continue;
            
    #reinitializing the dataset dictionary so that the objects for one flow are garbage collected
    for key in dataset:
        dataset[key]=[];   
           

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
        

def find_new_wind_location(velocity, counter,TOF, list_of_locations):
    """this function gives actual location coordinate after coriolis deflection takes place"""
    current_location = list_of_locations[counter];
    current_latitute = math.radians(current_location[0]);
    start_latitude = math.radians(list_of_locations[counter-1][0]);
    # total distance for one degree longitude in that latitude
    distance_for_one_degree_longitude = 111111 * math.cos(current_latitute);
    
    coriolis_acceleration = 2 * velocity * ((2 * math.pi) / 86400) * math.sin(start_latitude);
    # gives distance displaced in meters
    distance_displaced_due_to_coriolis = 0.5 * coriolis_acceleration * (TOF ** 2);
    # total degrees of deflection
    total_deflection_in_degree_of_longitude = float(distance_displaced_due_to_coriolis) / float(distance_for_one_degree_longitude);
    
    new_lon = float(current_location[1]) - total_deflection_in_degree_of_longitude;
    #normalizing longitude between +180 and -180
    if(new_lon >180):
        new_lon = (new_lon-180) -180
    elif(new_lon <-180):
        new_lon = (new_lon+180) + 180

    # returning deflected new coordinate of the location
    return (current_location[0], new_lon);
    
   
def write_to_csv_data(date,ID, source_id,destination_id,source_lat,source_lon,destination_lat,destination_lon, coordinates, velocity,dataset):
    """This function writes data for a particular stream flow into every single node data holder list for writing them later to hdfs""" 
    which_node_does_location_belong_to = find_node_location(coordinates);
    #adding line to more than one node if the line belongs to multiple over scanned region and only to one if not
    for n in which_node_does_location_belong_to:
        if(n == "node_1"):
            ID = ID+"_st";
            content = date+","+ID+","+source_id+","+destination_id+","+source_lat+","+source_lon+","+destination_lat+","+destination_lon+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity);  
            dataset[n].append(content)
        elif (n=="node_3"):
            ID = ID+"_th";
            content = date+","+ID+","+source_id+","+destination_id+","+source_lat+","+source_lon+","+destination_lat+","+destination_lon+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity);  
            dataset[n].append(content)
        elif(n=="node_2"):
            content = date+","+ID+","+source_id+","+destination_id+","+source_lat+","+source_lon+","+destination_lat+","+destination_lon+","+str(coordinates[0])+","+str(coordinates[1])+","+str(velocity);  
            dataset[n].append(content)
                

def find_node_location(coordinates):
    """this function returns a location where the wind line belongs to among all of the monitors based on mercator projection"""
    latitude = coordinates[0];
    longitude = coordinates[1];
    result = [];
    # need to be edited
    if(latitude>= -70.61 and latitude<= 70.61):
        result.append("node_2");
        #oversanned parts both on right side and left side
        if(longitude<-140 and longitude>=-180):
            result.append("node_3")
        elif (longitude >140 and longitude<=180):
            result.append("node_1");
            
    return result;
        
    
                
if __name__ == '__main__':
    import happybase;
    # configure the spark environment
    sparkConf = SparkConf().setAppName("Simulating Streamline");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = SparkContext(conf=sparkConf);
    sc.addPyFile("module.zip");      
#     from pywebhdfs.webhdfs import PyWebHdfsClient;
    distributed_dataset = sc.textFile("hdfs:/user/uacharya/subset_dataset_1934.txt",use_unicode=False,minPartitions=24);
    print("this is the driver container");
    # getting the header of the whole dataset
    header = distributed_dataset.first();
    # filtering the header out of the data 
    distributed_dataset = distributed_dataset.filter(lambda d: d != header);
    # mapping the data to prepare for processing
    data_in_required_format = distributed_dataset.map(create_required_datewise_data);
    data_in_required_format.cache();
    #collecting keys to do batch processing based on keys
    temp = set(data_in_required_format.keys().collect());
    print("total keys "+str(len(temp)));
    #sorting keys to create data in chronological order based on date
    sorted_keys = sorted(temp,key=int);
    #connecting to database for writing checker data
    database = happybase.ConnectionPool(size=1,host='cshadoop.boisestate.edu');
    #getting a connection from the pool
#    with database.connection() as db:
#        db.create_table('fChecker'.encode(),{'f'.encode():dict(max_versions=1,in_memory=True)});
    #creating batch processing with new rdd each iteration based on key values
    for key in sorted_keys[:2]:   
        print(key);
        keyed_rdd = data_in_required_format.filter(lambda t: t[0]==key).map(lambda t: t[1]).coalesce(48, shuffle=True);
        keyed_rdd.cache();
        #collecting all the dataset for broadcasting
        broadcast_data = keyed_rdd.collect();
        print(str(len(broadcast_data))+" driver program");
#        l = keyed_rdd.glom().map(len).collect()  # get length of each partition
#        print(min(l), max(l), sum(l)/len(l), len(l))  # check if skewed
#        broadcasting the entire keyed dataset
        broadcast_variable = sc.broadcast(broadcast_data);
        # analyzing the stations weather variables based on each date to create the simulation data for wind flow
        final_data_after_creating_files = keyed_rdd.treeReduce(create_data_from_station_data,depth=3);
        #x = final_data_after_creating_files.count();
        print("the final result for {0} is {1}".format(key,final_data_after_creating_files));
        # erasing the broadcast data set after use from everywhere
        broadcast_variable.unpersist(blocking=True);
        #erasing cached keyed rdd
        keyed_rdd.unpersist();

        global hbase;
        global hdfs;
        hbase = hdfs = None;#setting both global variables for each task to None so that these connection objects are only created inside executor and not serialized when sending task to them.
    
    sc.stop();
    

