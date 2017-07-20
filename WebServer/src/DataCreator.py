'''
Created on Jun 7, 2017

@author: Uzwal
'''
from threading import Thread;
import threading;
import math, csv, json;
from collections import defaultdict;
from numpy import interp;
import numpy as np;
from PIL import Image, ImageDraw;
import cStringIO,struct;
import cPickle, pickle,sys;
from base64 import b64encode;


"""Two global objects one for holding nested data for streamline based on flow ID between two stations and other for holding the
transformed pixel data of all lon and lat pair for streamline view in client side"""

mercator_projected_coordinates = None; 
nested_data_based_on_id = None;

class DataCreator():
    """Class which is responsible for creating the data in different formats and streaming to the client upon request"""
    def __init__(self):
        self.__raw_data_for_date = {};
        self.__canvas_data_for_date = {};
        self.__aggregated_data_for_date = {};
        self.lock = threading.Lock();
        
        
    def create_data_for_date(self, date, aggregation_width=None):
        """Function which gets the date for the data which is ready and initiates the process of creating of the data in different formats
           which could be requested by the client so that the respective data could be streamed"""
        # adding a key for the available date raw data
        self.__raw_data_for_date[date] = [];
        # add the indicator that raw data is ready for all the node for a date 
        for _index in range(0, 1):
            self.__raw_data_for_date[date].append({"indicator":"ready", "data":"/Users/Uzwal/Desktop/ineGraph/data1929.csv"});
        
        # adding a key for the date which bitmap data is to be created  
        self.__canvas_data_for_date[date] = [];
        # add the indicator that bitmap data is not ready for all the node for a date 
        for _index in range(0, 1):
            self.__canvas_data_for_date[date].append({"indicator":"not_ready", "data":None, 'frames':None});
            
        # adding a key for the date which aggregated data is to be created  
        self.__aggregated_data_for_date[date] = [];
        # add the indicator that bitmap data is not ready for all the node for a date 
        for _index in range(0, 1):
            self.__aggregated_data_for_date[date].append({"indicator":"not_ready", "data":None});
            
        # call the class that should create data in two additional formats
        agg_obj = DataInDifferentFormat(date, aggregate=self.__aggregated_data_for_date, lock=self.lock);
        agg_obj.start();
        
        bitmap_obj = DataInDifferentFormat(date, bitmap=self.__canvas_data_for_date, interpolation_width=0, lock=self.lock);
        bitmap_obj.start();

        # asking the main thread to sleep until the other threads are finished
        bitmap_obj.join();
        agg_obj.join();
        
            

    def check_available_data(self, date, raw=False, bitmap=False, aggregated=False):
        """ Function which checks if the data is available to stream to the client based on the parameters passed"""
        self.check_if_data_for_date_is_ready(date);
        
        if(raw == True):
            for node in self.__raw_data_for_date[date]:
                if node["indicator"] == "not_ready":
                    return "not_ready";
            
            return "ready";
        
        elif(bitmap == True):
            for node in self.__canvas_data_for_date[date]:
                if node["indicator"] == "not_ready":
                    return "not_ready";
            
            return "ready";
        
        elif(aggregated == True):
            for node in self.__aggregated_data_for_date[date]:
                if node["indicator"] == "not_ready":
                    return "not_ready";
            
            return "ready";
        
    def get_available_data(self, date, node, raw=False, bitmap=False, aggregated=False,PNG=False):
        """ Function which returns data to the caller based on the parameters passed and data availability"""
        self.check_if_data_for_date_is_ready(date);
        
        if(raw == True):
            data = self.__raw_data_for_date[date][node];
            if data["indicator"] == "not_ready":
                raise NotPresentError("data in this format is not ready");
            else:
                return data["data"];
            
        elif(bitmap == True):
            data = self.__canvas_data_for_date[date][node];
            if data["indicator"] == "not_ready":
                raise NotPresentError("data in this format is not ready");
            else:
                return data["data"];
        elif(PNG == True):
            data = self.__canvas_data_for_date[date][node];
            if data["indicator"] == "not_ready":
                raise NotPresentError("data in this format is not ready");
            else:
                return data["frames"];
        
        elif(aggregated == True):
            data = self.__aggregated_data_for_date[date][node];
            if data["indicator"] == "not_ready":
                raise NotPresentError("data in this format is not ready");
            else:
                return data["data"];
            
    def check_if_data_for_date_is_ready(self, date):
        if date not in self.__raw_data_for_date:
            raise NotPresentError("data for this date is not ready");
        
        
        
class NotPresentError(Exception):
    """ Class that raise exception when data request is either not available or has not been yet created"""
    def __init__(self, message):
        Exception.__init__(self);
        self.message = message;
        
class InvalidFormatError(Exception):
    """Class that raises exception when passed data is not a integer"""
    def __init__(self, message):
        Exception.__init__(self); 
        self.message = message;
        

class DataInDifferentFormat(Thread):
    """ This threaded class object is responsible for creating new data in different format as per the requirement"""
    def __init__(self, date, **kwargs):
        Thread.__init__(self);
        self.date = date;
        self.args = kwargs;
        
    def run(self):
        if("bitmap" in self.args):
            self.__read_transformed_coordinates_to_array();
            self.__create_PNG_images(self.args['interpolation_width'] if ('interpolation_width' in self.args) else 0);

        elif("aggregate" in self.args):
            global nested_data_based_on_id;
            # the file to process
            file_path = "/Users/Uzwal/Desktop/ineGraph/data" + str(self.date) + ".csv";
            # reading as dictionary all the csv rows so that the ones with same streamline ID can be grouped into one list
            reader = csv.DictReader(open(file_path, 'rb', 2048));
            # locking the object so that other thread only read it after the object is populated
            self.args["lock"].acquire();
            nested_data_based_on_id = defaultdict(list);
            # iterating over csv lines and grouping them according to same streamline ID
            for line in reader:
                nested_data_based_on_id[line["ID"]].append(line);   
            self.args["lock"].release();
            
            aggregated_output_data = {};  # local dictionary to write new aggregated data
            upper_bound = 0;  # getting the flow with the highest data points
            
            for key, data in nested_data_based_on_id.iteritems():
                total_data_points_for_a_flow = len(data);
                dist_between = self.__calculate_distance(float(data[0]["S_Lat"]), float(data[0]["S_Lon"]), float(data[0]["D_Lat"]) , float(data[0]["D_Lon"]));
                # aggregate the data based on whether data points are more than the distance between source and destination so that data points per km can be shown
                if(dist_between < total_data_points_for_a_flow):
                    step = int(round(float(total_data_points_for_a_flow) / dist_between));
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
            
            aggregated_output_data['upper_bound'] = upper_bound;
            
            self.__write_data_to_file(aggregated_output_data);  
                           
            
    def __calculate_distance(self, lat1, lon1, lat2, lon2):
        """This function calculates distance between two points in earth based on their lat and lon using great circle formula os sphere"""
        R = 6371;  # the radius of earth in km
        rad_lat1 = math.pi * lat1 / 180;
        rad_lat2 = math.pi * lat2 / 180;
        rad_diff_lat = math.pi * (lat2 - lat1) / 180;
        rad_diff_lon = math.pi * (lon2 - lon1) / 180;
        a = math.sin(rad_diff_lat / 2) ** 2 + math.cos(rad_lat1) * math.cos(rad_lat2) * (math.sin(rad_diff_lon / 2) ** 2);
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a));
        # distance between the two points in meters
        return round(R * c);
    
    def __write_data_to_file(self, obj):
        """This function writes the aggregated data in the form of dictionary to a json file for later use"""
        file_path = "/Users/Uzwal/Desktop/ineGraph/data_json_" + str(self.date) + ".json";
        # writing the data to a json file for each date
        with open(file_path, "w") as f:
            json.dump(obj, f);
        # updating the data dictionary that gives info about the data status
        for node in self.args["aggregate"][self.date]:
            node["indicator"] = "ready";
            node["data"] = file_path;
        print("aggregated finised");
            
    def __read_transformed_coordinates_to_array(self):
        """This function reads the mercator transformed coordinates from file to array only for first time the 
        server starts to intialize list to use"""   
        global mercator_projected_coordinates;
        # only when list is empty
        if(mercator_projected_coordinates == None):
            mercator_projected_coordinates = [];
            with open("/Users/Uzwal/Desktop/ineGraph/projected_coord_data.txt", "rb") as read_file:
                for line in read_file:
                    contents = line.split();
                    mercator_projected_coordinates.append(contents);
    
               
    def __create_PNG_images(self, interpolation_width): 
        """This function reads the streamline data and created images for all 60 frames based on the data"""
        # dictionary to hold the images data and every path data to stream to client
        path_stream_data = {'stations':[], 'path':[]};
        # list to store all the lines data to draw in bitmap later on
        bitmap_data = [];
        checker = set();
        
        x_end_points_in_view = (-3898.2296905911007, 4898.229690591101);
        # getting lock for the global dictionary that holds the flow data
        self.args["lock"].acquire();
        # normalizing arrow size based on the number of lines in a particular flow
        upper_bound = len(nested_data_based_on_id[max(nested_data_based_on_id, key=lambda x: len(nested_data_based_on_id[x]))]);
        # iterating through every flow between two stations
        for key, value in nested_data_based_on_id.iteritems():
            arrow_size = interp(len(value), [1, upper_bound], [10, 1]);  # getting size of arrow based on the number of points in a flow
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
            
            flow_data = []; #list for holding data for one flow  
            for i in range(len(value)):
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
                        obj = (start_point[0],start_point[1],end_point[0], end_point[1],velocity);
                        flow_data.append(obj);
                        # creating the required data to draw lines in image file
                        path = {"start":start_point, "interpolate":path_interpolator, "before":before_angle, "after":after_angle, "h":hypo};
                        bitmap_data.append(path);
            
            path_stream_data['path'].append({key:flow_data,'h':arrow_size});
            #removing binding for this flow data
            del flow_data;
                       
        self.args["lock"].release();
        
        del checker;  # clearing out the set as it is no longer needed
        
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
        PNG_stream_data = [];  # list to hold all the PNG images data
        for frame in range(1,61):
            t = float(frame * 16) / float(1000);
            img = Image.new("RGBA", (1280, 800), color=(0, 0, 0, 0));
            draw = ImageDraw.Draw(img);
            # drawing all the lines first in one loop
            for line in bitmap_data:
                a = line['start'];
                c = line['interpolate'](t);
                x1, y1 = math.ceil(c[0]), math.ceil(c[1]);
                draw.line([a[0], a[1], x1, y1], fill="#FD5959", width=1);
                  
                line['end'] = (x1, y1);
            # drawing all arrow heads in one loop
            for line in bitmap_data:
                x1, y1 = line['end'][0], line['end'][1];
                left_x = math.ceil(x1 + math.cos(line['before']) * line['h']);
                left_y = math.ceil(y1 + math.sin(line['before']) * line['h']);
                right_x = math.ceil(x1 + math.cos(line['after']) * line['h']);
                right_y = math.ceil(y1 + math.sin(line['after']) * line['h']);
                draw.polygon([x1, y1, left_x, left_y, right_x, right_y, x1, y1], fill="white", outline="white")
#             img.save("/Users/Uzwal/Desktop/imgs/"+str(frame)+".png","PNG");
            buf_string = cStringIO.StringIO();
            img.save(buf_string, format="PNG", quality=100);
#             img_str = b64encode(buf_string.getvalue());
#             path_stream_data['frames'].append(img_str);
            PNG_stream_data.append(struct.pack('!H',buf_string.tell())+buf_string.getvalue());
            buf_string.close();
               
        # indicating to the dictionary that holdes all the data that bitmap data for particular date is ready now
        for node in self.args['bitmap'][self.date]:
#             node["data"] = pickle.dumps(path_stream_data, protocol=1);
            node['frames'] = PNG_stream_data;
            node["data"] = json.dumps(path_stream_data);
            node["indicator"] = "ready";

        
        print("bitmap finished"); 
            
            
   
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
        upper_coordinates = mercator_projected_coordinates[upper_i][upper_j].split(",");
        lower_coordinates = mercator_projected_coordinates[lower_i][lower_j].split(",");
        # getting the difference between two points in the range to interpolate the pixels in both x and y directions
        x_diff = abs(float(lower_coordinates[0]) - float(upper_coordinates[0]));
        y_diff = abs(float(lower_coordinates[1]) - float(upper_coordinates[1]));
        
        start_coordinates = mercator_projected_coordinates[starting_y][starting_x].split(",");
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
              
         
            
        
            
        
        

        
    
    
    
    
    
    
    
    
    
    
