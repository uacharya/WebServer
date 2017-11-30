'''
Created on Aug 25, 2017

@author: walluser
'''
from multiprocessing import Process;
import math, csv;
from collections import defaultdict;
from numpy import interp;
import numpy as np;
from PIL import Image, ImageDraw;
import cPickle,os;

class DataInDifferentFormat(Process):
    """ This sub process class object is responsible for creating new data in different format as per the requirement"""

    def __init__(self, date,node, **kwargs):
        Process.__init__(self);
        self.date = date;
        self.node = node;
        self.args = kwargs;
        
    def run(self):
        if("bitmap" in self.args):
            self.__create_PNG_images(self.args['interpolation_width'] if ('interpolation_width' in self.args) else 0);

        elif("aggregate" in self.args):
            # the file to process
            file_path = "C:\\D3\\temp\\"+str(self.date)+"\\node"+str(self.node)+"\\output.csv";
            # reading as dictionary all the csv rows so that the ones with same streamline ID can be grouped into one list
            reader = list(csv.DictReader(open(file_path, 'rb', 2048)));
            #checking if the file is empty
            if(len(reader)==0):
                self.__write_data_to_file("");
                return;
    
            nested_data_based_on_id = defaultdict(list);  # for holding nested data for streamline based on flow ID between two stations
            # iterating over csv lines and grouping them according to same streamline ID
            for line in reader:
                nested_data_based_on_id[line["ID"]].append(line);   
                            
            aggregated_output_data = {};  # local dictionary to write new aggregated data
            upper_bound = 0;  # getting the flow with the highest data points
            
            for key, data in nested_data_based_on_id.iteritems():
                total_data_points_for_a_flow = len(data);
                dist_between_in_degrees = self.__get_diff_in_coord(data[0],data[total_data_points_for_a_flow-1]);

                # aggregate the data based on whether data points are more than the distance between source and destination so that data points per km can be shown
                if(dist_between_in_degrees < total_data_points_for_a_flow):
                    step = int(round(float(total_data_points_for_a_flow) / dist_between_in_degrees));
                    if(step > 1):
                        # creating aggregated data from the old data based on step size calculated
                        temp = data[::step];
                        if((total_data_points_for_a_flow - 1) % step != 0):
                            temp.append(data[total_data_points_for_a_flow - 1]); 
                        aggregated_output_data[key] = temp;
                        
                        if(len(temp) > upper_bound):
                            upper_bound = len(temp);
                    else:
                        aggregated_output_data[key] = data;
                        
                        if(total_data_points_for_a_flow > upper_bound):
                            upper_bound = total_data_points_for_a_flow;
                        
                else: 
                    aggregated_output_data[key] = data;
                    
                    if(total_data_points_for_a_flow > upper_bound):
                        upper_bound = total_data_points_for_a_flow;
            
            aggregated_output_data['upper_bound'] = upper_bound;
            
            del nested_data_based_on_id;  # removing binding from the dict as it is no longer needed
            
            self.__write_data_to_file(aggregated_output_data);  
                           
         
    def __get_diff_in_coord(self,entry,exit):
        """This function gets the aggregation number of points for a streamline in a node"""      
        lat1,lon1 = float(entry["Wind_Lat"]),float(entry["Wind_Lon"]);
        lat2,lon2 = float(exit["Wind_Lat"]),float(exit["Wind_Lon"]);
        
        lat_diff = lat1-lat2 if lat1>=lat2 else lat2-lat1;
        lon_diff = lon1-lon2 if lon1>=lon2 else lon2-lon1;
        
        return lat_diff if lat_diff>=lon_diff else lon_diff;
            
    def __write_data_to_file(self, obj):
        """This function writes the aggregated data in the form of dictionary to a json file for later use"""
        file_path = "C:\\D3\\temp\\agg\\data_json_" + str(self.date) +"_"+str(self.node)+ ".json";
        # writing the data to a json file for each date
        with open(file_path, "wb") as f:
            cPickle.dump(obj, f,protocol=cPickle.HIGHEST_PROTOCOL);
        # updating the data dictionary that gives info about the data status
        self.args["aggregate"].put({"d":self.date,'n':self.node,'agg':True,'p':file_path});
        print("aggregated finised");    
               
    def __create_PNG_images(self, interpolation_width): 
        """This function reads the streamline data and created images for all 60 frames based on the data"""
        # dictionary to hold the images data and every path data to stream to client
        path_stream_data = {'stations':[], 'path':[]};
        # list to store all the lines data to draw in bitmap later on
        bitmap_data = [];
        checker = set();
        x_end_points_in_view = (0,11520);
        # the file to process
        file_path = "C:\\D3\\temp\\"+str(self.date)+"\\node"+str(self.node)+"\\output.csv";
        # reading as dictionary all the csv rows so that the ones with same streamline ID can be grouped into one list
        reader = csv.DictReader(open(file_path, 'rb', 2048));
        # for holding nested data for streamline based on flow ID between two stations
        nested_data = defaultdict(list); 
        # iterating over csv lines and grouping them according to same streamline ID
        for line in reader:
            nested_data[line["ID"]].append(line); 
        # normalizing arrow size based on the number of lines in a particular flow
        upper_bound = len(nested_data[max(nested_data, key=lambda x: len(nested_data[x]))]);
        # iterating through every flow between two stations
        for key, value in nested_data.iteritems():
            arrow_size = interp(len(value), [1, upper_bound], [10, 5]);  # getting size of arrow based on the number of points in a flow
#             print(key,arrow_size);
            station_data = value[0];
            # adding the stations data to show stations in the map
            source_station = (station_data['Source'], station_data['S_Lat'], station_data['S_Lon']);
            destination_station = (station_data['Destination'], station_data['D_Lat'], station_data['D_Lon']);
            # adding only unique elements to stations data
            if source_station[0] not in checker:
                path_stream_data['stations'].append(source_station);
                checker.add(source_station[0]);
            
            if destination_station[0] not in checker:
                path_stream_data['stations'].append(destination_station);
                checker.add(destination_station[0]);
            
            flow_data = [];  # list for holding data for one flow  
            for i in xrange(len(value)):
                if(i == len(value) - 1):
                    break;
                x0 = self.__project_points_to_mercator(float(value[i]['Wind_Lon']), float(value[i]['Wind_Lat']));
                x1 = self.__project_points_to_mercator(float(value[i + 1]['Wind_Lon']), float(value[i + 1]['Wind_Lat']));
                temp = self.__tween_the_curves(value[i], value[i + 1], x0, x1, x_end_points_in_view[0], x_end_points_in_view[1]);
                # creating additional lines on each side of a line to show more data
                for line in temp:
                    interpolated_particles = self.__create_random_particles(line, i, interpolation_width);
                    for particle in interpolated_particles:
                        start_point = [math.ceil(particle[0][0]), math.ceil(particle[0][1])];
                        end_point = [math.ceil(particle[1][0]), math.ceil(particle[1][1])];
                        path_interpolator = self.__interpolate_array(start_point, end_point);
                        angle = math.atan2(end_point[1] - start_point[1], end_point[0] - start_point[0]);
                        velocity = value[i + 1]['Wind_Velocity'];
                        move_angle = 30 * (math.pi / 180);
                        before_angle = (math.pi + angle) - move_angle;
                        after_angle = (math.pi + angle) + move_angle;
                        hypo = abs(10 / math.cos(move_angle));
                        # creating an object and appending its info to stream to client
                        obj = (start_point[0], start_point[1], end_point[0], end_point[1], velocity);
                        flow_data.append(obj);
                        # creating the required data to draw lines in image file
                        path = {"start":start_point, "interpolate":path_interpolator, "before":before_angle, "after":after_angle, "h":hypo};
                        bitmap_data.append(path);
            
            path_stream_data['path'].append({key:flow_data, 'h':arrow_size});
            # removing binding for this flow data
            del flow_data;
       
        del checker;  # removing binding from the set as it is no longer needed
        self.__draw_images(bitmap_data, path_stream_data);
        
        
    def __interpolate_array(self, a, b):
        """This function interpolates an array between two arrays based on the normalized fraction passed between 0 to 1"""
        c = [];
        for i in range(len(a)):
            c.append(self.__value(a[i], b[i]));
        def output(t):
            o = [];
            for f  in c: 
                o.append(f(t));
            return o;
                
        return output;
    
    def __value(self, a, b):
        # raising exception just in case the list has other than float object
        if(not isinstance(a, float)):
            raise InvalidFormatError("the passed data is not a number");
        
        b = b - a;
        def inter(t):
            return a + b * t;
        return inter;
    
        
    def __draw_images(self, bitmap_data, path_stream_data):
        """This function creates PNG images for each frame of streamline flow animation and stores them in stream data to stream later on"""
        dir_path = "C:\\D3\\temp\\bitmap\\"+str(self.date)+"\\"+str(self.node);
        #checking if the directory exists or not
        if not(os.path.exists(dir_path)):
            os.makedirs(dir_path+"\\imgs");
        
        #object to transform pixel coordinates on svg to equivalent canvas coordinates overlayed on top of it based on the node
        transformer = NodeCoordinateTransformer(self.node);
            
        for frame in range(1, 31):
            t = float(frame * 33.33) / float(1000);
            img = Image.new("RGBA", (3840, 2160), color=(0, 0, 0, 0));
            draw = ImageDraw.Draw(img);
            # drawing all the lines first in one loop
            for line in bitmap_data:
                a = line['start'];
                c = line['interpolate'](t);
                x1, y1 = math.ceil(c[0]), math.ceil(c[1]);
                #converting the coordinates for a node overlayed canvas
                start = transformer.convert_to_actual_XY(a[0], a[1]);
                end = transformer.convert_to_actual_XY(x1, y1);
                
                draw.line([start[0], start[1],end[0], end[1]], fill="#FD5959", width=1);
                line['end'] = (x1,y1);
                
            # drawing all arrow heads in one loop
            for line in bitmap_data:
                x1, y1 = line['end'][0], line['end'][1];
                left_x = math.ceil(x1 + math.cos(line['before']) * line['h']);
                left_y = math.ceil(y1 + math.sin(line['before']) * line['h']);
                right_x = math.ceil(x1 + math.cos(line['after']) * line['h']);
                right_y = math.ceil(y1 + math.sin(line['after']) * line['h']);
                
                end = transformer.convert_to_actual_XY(x1, y1);
                left= transformer.convert_to_actual_XY(left_x, left_y);
                right= transformer.convert_to_actual_XY(right_x, right_y);
                
                draw.polygon([end[0], end[1], left[0], left[1], right[0], right[1], end[0],end[1]], fill="white", outline="white");
                
            img.save(dir_path+"\\imgs\\" + str(frame) + ".png", "PNG", quality=100);

        file_path = dir_path+"\\data.json";
        # writing the data to a json file for each date
        with open(file_path, "wb") as f:
            cPickle.dump(path_stream_data, f, protocol=cPickle.HIGHEST_PROTOCOL);

        self.args["bitmap"].put({"d":self.date,'n':self.node,"bmp":True,'p':dir_path});
        print("bitmap finished for "+str(self.node)); 
            

    def __project_points_to_mercator(self, lon, lat):
        """ This function tries to project the lon and lat to mercator projection as close as possible to d3's mercator projection"""
        lon_zero_degree_index = 180;  # index number in the array which holds value for zero degree longitude
        lat_zero_degree_index = 89;  # index number in the array which holds value for zero degree latitude
        # starting points where offset is added to get the exact degree with decimal projection
        starting_x = starting_y = x_frac = y_frac = None; 
        lower_i = lat_zero_degree_index - int(lat);
        lower_j = lon_zero_degree_index + int(lon);
        
        if(lat < 0):
            upper_i = lower_i + 1;
            starting_y = lower_i;
            if(int(lat) == 0):
                y_frac = abs(lat);
            else:     
                y_frac = abs(lat % int(lat));
        else:
            upper_i = lower_i - 1;
            starting_y = upper_i;
            if(int(lat) == 0):
                y_frac = 1 - abs(lat);
            else:
                y_frac = 1 - abs(lat % int(lat)); 
            
        if(lon < 0):
            upper_j = lower_j - 1;
            starting_x = upper_j;
            if(int(lon) == 0):
                x_frac = 1 - abs(lon);
            else:    
                x_frac = 1 - abs(lon % int(lon));
        else:
            upper_j = lower_j + 1;
            starting_x = lower_j;
            if(int(lon) == 0):
                x_frac = abs(lon);
            else:
                x_frac = abs(lon % int(lon));

        # two ends of a interpolation spectrum  
        upper_coordinates = self.args["projection_coord"][upper_i][upper_j].split(",");
        lower_coordinates = self.args["projection_coord"][lower_i][lower_j].split(",");
        # getting the difference between two points in the range to interpolate the pixels in both x and y directions
        x_diff = abs(float(lower_coordinates[0]) - float(upper_coordinates[0]));
        y_diff = abs(float(lower_coordinates[1]) - float(upper_coordinates[1]));
        
        start_coordinates = self.args["projection_coord"][starting_y][starting_x].split(",");
        return (float(start_coordinates[0]) + (x_frac * x_diff), float(start_coordinates[1]) + (y_frac * y_diff));
    
    def __tween_the_curves(self, a, b, x0, x1, min_x, max_x):
        """This function creates new parts of a curve based on their location in the new projection if 
        their pixel coordinates conform to their lon in degrees """
        result = [];  # list to hold the data for the lines
    
        if ((float(b['Wind_Lon']) >= float(a['Wind_Lon'])) and (x1[0] < x0[0])):
            middle_point = self.__get_the_connection_point(x0, x1, "r", min_x, max_x);
            connecting_point = [max_x, middle_point];
            next_one = [min_x, middle_point];
    
            result.append([x0, connecting_point]);
            result.append([next_one, x1]);
        elif ((float(b["Wind_Lon"]) < float(a["Wind_Lon"])) and (x1[0] >= x0[0])):
            middle_point = self.__get_the_connection_point(x0, x1, "l", min_x, max_x);
            connecting_point = [min_x, middle_point];
            next_one = [max_x, middle_point];
    
            result.append([x0, connecting_point]);
            result.append([next_one, x1]);
        else:
            result.append([x0, x1]);
          
        return result;
    
    def __get_the_connection_point(self, x0, x1, flag, min_x, max_x):
        """This function gets the exact location point where two points of a line should be connected just in case
        their pixel coordinates do not conform with their lon location in degrees""" 
        if (flag == "r"):
            base = abs(x0[0] - max_x) + abs(min_x - x1[0]);
            height = x0[1] - x1[1];
            tan_ratio = height / base;
            return x0[1] - (abs(x0[0] - max_x) * tan_ratio);
        elif (flag == "l"):
            base = abs(x0[0] - min_x) + abs(max_x - x1[0]);
            height = x0[1] - x1[1];
            tan_ratio = height / base;
            return x1[1] + (abs(x1[0] - max_x) * tan_ratio);
        
    def __create_random_particles(self, value, index, interpolation_width):
        """This function interpolates parallel lines on both side of the given line within the interpolation width"""
        run = value[1][0] - value[0][0];
        rise = value[0][1] - value[1][1];
        tan_ratio = rise / run;
        y_max = value[0][1] + interpolation_width;
        y_min = value[0][1] - interpolation_width;
        x_max = value[0][0] + interpolation_width;
        x_min = value[0][0] - interpolation_width;
        offset = 5;
        particles = [];
        
        # when the slope of line is greater than 45 degrees
        if (abs(math.atan(tan_ratio) * (180 / math.pi)) > 45):
            # this randomly creates all the lines on the right
            for i in np.arange(value[0][0] + offset, x_max + 1, +offset) :
                temp = self.__plot_the_lines_for_x(i, tan_ratio, value[0][1], value[1][1]);
                particles.append(temp);
                if (index == 0):
                    particles.append([[value[0][0], value[0][1]], [temp[0][0], temp[0][1]]]);
                    
            # this randomly creates all the lines on the right
            for i in np.arange(value[0][0] - offset, x_min - 1, -offset):
                temp = self.__plot_the_lines_for_x(i, tan_ratio, value[0][1], value[1][1]);
                particles.append(temp);
                if (index == 0):
                    particles.append([[value[0][0], value[0][1]], [temp[0][0], temp[0][1]]]);
                
        else:
            # this randomly creates all the lines on the top
            for i in np.arange(value[0][1] - offset, y_min - 1, -offset) :
                temp = self.__plot_the_lines_for_y(i, tan_ratio, value[0][0], value[1][0]);
                particles.append(temp);
                if (index == 0):
                    particles.append([[value[0][0], value[0][1]], [temp[0][0], temp[0][1]]]);
                  
            # /this randomly creates all the lines on the bottom
            for i in np.arange(value[0][1] + offset, y_max + 1, +offset) :
                temp = self.__plot_the_lines_for_y(i, tan_ratio, value[0][0], value[1][0]);
                particles.append(temp);
                if (index == 0):
                    particles.append([[value[0][0], value[0][1]], [temp[0][0], temp[0][1]]]);
                

        # adding the actual line 
        particles.append(value);

        return particles;
    
    def __plot_the_lines_for_x(self, x1, ratio, y1, y2):
        temp = [];
        x2 = ((y1 - y2) / ratio) + x1;
        temp.append([x1, y1]);
        temp.append([x2, y2]);
        return temp;
    
    def __plot_the_lines_for_y(self, y1, ratio, x1, x2):
        temp = [];
        y2 = y1 - (ratio * (x2 - x1));
        temp.append([x1, y1]);
        temp.append([x2, y2]);
        return temp;
    
    
class NodeCoordinateTransformer(object):
    """ This class object will be resposible for normalizing data in map coordinates to canvas coordinate for individual nodes """
    #tuple that holds starting x and y coordinates for each node in wall
    node_starting_dimensions = ((0,0),(3840,0),(7680,0),(0,2160),(3840,2160),(7680,2160),(0,4320),(3840,4320),(7680,4320));
    
    def __init__(self,node_id):
        """ This function intializes the starting coordinates for this node based on the node id"""
        self.start_x = NodeCoordinateTransformer.node_starting_dimensions[node_id-1][0];
        self.start_y = NodeCoordinateTransformer.node_starting_dimensions[node_id-1][1];
        
    
    def convert_to_actual_XY(self,x,y):
        """ This function takes a coordinate value for a node and converts that into coordinate value for canvas overlayed on svg on that  node"""
        temp=[];
        
        temp.append(x-self.start_x);
        temp.append(y-self.start_y);
        
        return temp;
        
    
class InvalidFormatError(Exception):
    """Class that raises exception when passed data is not a integer"""
    def __init__(self, message):
        Exception.__init__(self); 
        self.message = message;
        
#                 diff_type = self.__find_aggregated_points_between(float(data[0]["S_Lat"]), float(data[0]["S_Lon"]), float(data[0]["D_Lat"]) , float(data[0]["D_Lon"]));
#         
#                 if(diff_type=="lat"):
#                     dist_between_in_degrees = self.__get_diff_in_coord(data[0],data[total_data_points_for_a_flow-1],lat=True);
#                 else:
#                     dist_between_in_degrees = self.__get_diff_in_coord(data[0],data[total_data_points_for_a_flow-1],lon=True);         
#     def __find_aggregated_points_between(self,lat1,lon1,lat2,lon2):
#         """This function finds the difference in degrees between two stations and returns the largest difference between longitude difference or latitude difference"""
#         lat_diff = lat1-lat2 if lat1>=lat2 else lat2-lat1;
#         lon_diff = lon1-lon2 if lon1>=lon2 else lon2-lon1;
# 
#         return "lat" if lat_diff>=lon_diff else "lon";
            
    
#     def __get_limit_of_this_node(self,node,upper_lat=False,lower_lat=False,upper_lon=False,lower_lon=False):
#         """This function returns bound of a node in terms of lat and lon as asked by user"""
#         temp  =self.node_bounds[node-1];
#         if(lower_lat):
#             return temp[0];
#         elif(lower_lon):
#             return temp[1];
#         elif (upper_lat):
#             return temp[2]
#         elif (upper_lon):
#             return temp[3];
#             
#     def __find_node_location(self,latitude,longitude):
#         """this function returns a location where the wind line belongs to among all of the monitors based on mercator projection"""
# 
#         if((latitude <= 79 and latitude >= 54.548) and(longitude >= -180 and longitude <= -60.021)):
#             return 1;
#         elif((latitude <= 79 and latitude >= 54.548) and(longitude >=-60 and longitude <= 59.989)):
#             return 2;
#         elif((latitude <= 79 and latitude >= 54.548) and(longitude >=60 and longitude <= 180)):
#             return 3;
#         elif((latitude <=54.52 and latitude >= -2.155) and(longitude >= -180 and longitude <= -60.021)):
#             return 4;
#         elif((latitude <=54.52 and latitude >= -2.155) and(longitude >= -60 and longitude <= 59.989)):
#             return 5;
#         elif((latitude <=54.52 and latitude >= -2.155) and(longitude >= 60 and longitude <= 180)):
#             return 6;
#         elif((latitude <=-2.187 and latitude >= -56.97) and(longitude >= -180 and longitude <= -60.021)):
#             return 7;
#         elif((latitude <=-2.187 and latitude >= -56.97) and(longitude >= -60 and longitude <= 59.989)):
#             return 8;
#         elif((latitude <=-2.187 and latitude >= -56.97) and(longitude >= 60 and longitude <= 180)):
#             return 9;
