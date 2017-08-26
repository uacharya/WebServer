'''
Created on Jun 7, 2017

@author: Uzwal
'''
from threading import Thread;
from multiprocessing import Pipe;
from DataWriter import DataInDifferentFormat,InvalidFormatError;
import cStringIO,struct,json;
import cPickle,msgpack;
from PIL import Image; 
from base64 import b64encode;

class DataCreator(object):
    """Class which is responsible for creating the data in different formats and streaming to the client upon request"""
    mercator_projected_coordinates=None #class variable for holding the transformed pixel data of all lon and lat pair for streamline view in client side
    
    def __init__(self):
        self.__raw_data_for_date = {};
        self.__canvas_data_for_date = {};
        self.__aggregated_data_for_date = {};
        
    def create_data_for_date(self, date, aggregation_width=None):
        """Function which gets the date for the data which is ready and initiates the process of creating of the data in different formats
           which could be requested by the client so that the respective data could be streamed"""
        # adding a key for the available date raw data
        self.__raw_data_for_date[date] = [];
        # add the indicator that raw data is ready for all the node for a date 
        for _index in xrange(0, 1):
            self.__raw_data_for_date[date].append({"indicator":"ready", "data":"C:\\Users\\walluser\\Desktop\\testing\\data1929.csv"});
        
        # adding a key for the date which bitmap data is to be created  
        self.__canvas_data_for_date[date] = [];
        # add the indicator that bitmap data is not ready for all the node for a date 
        for _index in xrange(0, 1):
            self.__canvas_data_for_date[date].append({"indicator":"not_ready", "data":None, 'frames':None});
            
        # adding a key for the date which aggregated data is to be created  
        self.__aggregated_data_for_date[date] = [];
        # add the indicator that bitmap data is not ready for all the node for a date 
        for _index in xrange(0, 1):
            self.__aggregated_data_for_date[date].append({"indicator":"not_ready", "data":None});
    
        #creating pipe to communicate between two proceses
        agg_parent_conn,agg_child_conn = Pipe(duplex=False);
        # call the class that should create data in two additional formats
        agg_obj = DataInDifferentFormat(date,0, aggregate=agg_child_conn);
        agg_obj.start();
        
        #creating pipe to communicate between two proceses
        bitmap_parent_conn,bitmap_child_conn = Pipe(duplex=False);
        bitmap_obj = DataInDifferentFormat(date,0, bitmap=bitmap_child_conn,projection_coord= DataCreator.mercator_projected_coordinates,interpolation_width=0);
        bitmap_obj.start();
        
        try:
            agg_response = agg_parent_conn.recv()
            agg_parent_conn.close();
            agg_date,agg_node,agg_path = agg_response['d'],agg_response['n'],agg_response['p'];
        except EOFError:
            pass;
        try:
            bitmap_response = bitmap_parent_conn.recv();
            bitmap_parent_conn.close();
            bitmap_date,bitmap_node,bitmap_path= bitmap_response['d'],bitmap_response['n'],bitmap_response['p'];
        except EOFError:
            pass;
        agg_obj.join();
        bitmap_obj.join();
        #single thread outperforming in this context
        import datetime;
        start = datetime.datetime.now();
        agg_thread = ReadIntoMemory(self.__aggregated_data_for_date[agg_date][agg_node],agg_path,agg=True);
        agg_thread.start();
        bitmap_thread = ReadIntoMemory(self.__canvas_data_for_date[bitmap_date][bitmap_node],bitmap_path,bitmap=True);
        bitmap_thread.start();
        # asking the main thread to sleep until the other processes are finished
        agg_thread.join();
        bitmap_thread.join();
        end = datetime.datetime.now();
        diff = end - start;
        elapsed_ms = (diff.days * 86400000) + (diff.seconds * 1000) + (diff.microseconds / 1000);
        print(elapsed_ms);
        
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
        
    @classmethod
    def read_transformed_coordinates_to_array(cls):
        """This function reads the mercator transformed coordinates from file to array only for first time the 
        server starts to intialize list to use"""   
        # only when list is empty
        if(cls.mercator_projected_coordinates == None):
            cls.mercator_projected_coordinates = [];
            with open("C:\\Users\\walluser\\Desktop\\testing\\projected_coord_data.txt", "rb") as read_file:
                for line in read_file:
                    contents = line.split();
                    cls.mercator_projected_coordinates.append(contents);
        
class NotPresentError(Exception):
    """ Class that raise exception when data request is either not available or has not been yet created"""
    def __init__(self, message):
        Exception.__init__(self);
        self.message = message;
        


class ReadIntoMemory(Thread):
    """This class takes the data from the disk and writes in the memory after data has been created my multiple processes one for each node"""
    def __init__(self,data_dict,path,**kwargs):
        Thread.__init__(self);
        self.data_holder =data_dict;
        self.path = path;
        self.arg = kwargs;
  
    def run(self):
        if ("agg" in self.arg):
            #reading from a file to memory to stream later
            with open(self.path,"rb") as f:
                self.data_holder['data'] = json.dumps(cPickle.load(f));
            #indicating that reading in memory is finished for this data  
            self.data_holder["indicator"]='ready';  
            
        elif("bitmap" in self.arg):
            #putting the line data into a object to stream
            with open(self.path+"\\data.json","rb")as f:
                self.data_holder['data'] = json.dumps(cPickle.load(f));

            content_length =0; #calculate the content length in bytes of all images to stream in total
            PNGS=[]; #list to hold all the pngs data in memory
            #reading all the images to memory to stream
            for x in xrange(1,61):
                buf_string = cStringIO.StringIO();
                Image.open(self.path+"\\imgs\\"+str(x)+".png").save(buf_string, format="PNG", quality=100);
                content_length = content_length+(buf_string.tell()+2); 
                PNGS.append(struct.pack('!H',buf_string.tell())+buf_string.getvalue());
                buf_string.close();
                
            self.data_holder['frames']=(content_length,PNGS);
            #indicating that reading in memory is finished for this data  
            self.data_holder["indicator"]='ready'; 
                
        else:
            raise InvalidFormatError("the type of format is not available to read in memory");
            
                  
#             buf_string = cStringIO.StringIO();
#             img.save(buf_string, format="PNG", quality=100);
#             img_str = b64encode(buf_string.getvalue());
#             path_stream_data['frames'].append(img_str);
#             PNG_stream_data.append(struct.pack('!H',buf_string.tell())+buf_string.getvalue());
#             content_length = content_length+(buf_string.tell()+2); 
#             path_stream_data['frames'].append(buf_string.getvalue());
#             buf_string.close();
               
#         node['frames'] = (content_length,PNG_stream_data);
#         node["data"] = (json.dumps(path_stream_data),(content_length,PNG_stream_data));
#         node['data'] = msgpack.packb(path_stream_data,use_bin_type=True);
#         node['data'] = json.dumps(path_stream_data);
        

        
    
    
    
    
    
    
    
    
    
    
