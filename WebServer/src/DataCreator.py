'''
Created on Jun 7, 2017

@author: Uzwal
'''
from threading import Thread;
from multiprocessing import Pipe, Queue;
from DataWriter import DataInDifferentFormat,InvalidFormatError;
import cStringIO,struct,json;
import cPickle;
from PIL import Image; 
from base64 import b64encode;
import msgpack;

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
        for index in xrange(0, 9):
            self.__raw_data_for_date[date].append({"indicator":"ready", "data":None});
        #array containing all the threaded objects reading files into memory
#         list_of_threads=[];  
#         for index in xrange(0,9):
#             raw_thread= ReadIntoMemory(self.__raw_data_for_date[date][index],"C:\\D3\\temp\\"+str(date)+"\\node"+str(index+1)+"\\output.csv",raw=True);
#             raw_thread.start();
#             list_of_threads.append(raw_thread); 
# #         
#         for t in list_of_threads:
#             t.join();
        # adding a key for the date which bitmap data is to be created  
        self.__canvas_data_for_date[date] = [];
        # add the indicator that bitmap data is not ready for all the node for a date 
        for _ in xrange(0, 9):
            self.__canvas_data_for_date[date].append({"indicator":"not_ready", "data":None, 'frames':None});
            
        # adding a key for the date which aggregated data is to be created  
        self.__aggregated_data_for_date[date] = [];
        # add the indicator that bitmap data is not ready for all the node for a date 
        for _ in xrange(0, 9):
            self.__aggregated_data_for_date[date].append({"indicator":"not_ready", "data":None});
                        
        list_of_processes=[];
        
        q= Queue();
        # call the class that should create data in two additional formats
        for i in range(9):
            agg_obj = DataInDifferentFormat(date,i+1, aggregate=q);
            agg_obj.start();
            list_of_processes.append(agg_obj);
                           
        list_of_threads=[];      
        import datetime;
        start = datetime.datetime.now();
        counter=1;
        while counter<=len(list_of_processes):
            try:
                response = q.get();
                res_date,res_node,res_path = response['d'],response['n'],response['p'];
                if('agg' in response):
                    agg_thread=ReadIntoMemory(self.__aggregated_data_for_date[res_date][res_node-1],res_path,agg=True);
                    agg_thread.start();
                    list_of_threads.append(agg_thread);
                    #starting a new process that creates bitmap data based on previously aggregated data 
                    bitmap_obj = DataInDifferentFormat(res_date,res_node, bitmap=q,projection_coord= DataCreator.mercator_projected_coordinates,interpolation_width=0);
                    bitmap_obj.start();
                    list_of_processes.append(bitmap_obj);
                    
                elif('bmp' in response):
                    bitmap_thread = ReadIntoMemory(self.__canvas_data_for_date[res_date][res_node-1],res_path,bitmap=True);
                    bitmap_thread.start()
                    list_of_threads.append(bitmap_thread);
                counter+=1;
            except Exception as e:
                print(e.message);
          
        for t in range(len(list_of_threads)):
            list_of_threads[t].join();
              
        end = datetime.datetime.now();
        diff = end - start;
        elapsed_ms = (diff.days * 86400000) + (diff.seconds * 1000) + (diff.microseconds / 1000);
        print(elapsed_ms);  
        for i in range(len(list_of_processes)):
            list_of_processes[i].join();
        
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
        
    def get_available_data(self, date, node, raw=False, bitmap_json=False, aggregated=False,bitmap_PNG=False):
        """ Function which returns data to the caller based on the parameters passed and data availability"""
        self.check_if_data_for_date_is_ready(date);
        
        if(raw == True):
            data = self.__raw_data_for_date[date][node-1];
            if data["indicator"] == "not_ready":
                raise NotPresentError("data in this format is not ready");
            else:
                return data["data"];
            
        elif(bitmap_json == True):
            data = self.__canvas_data_for_date[date][node-1];
            if data["indicator"] == "not_ready":
                raise NotPresentError("data in this format is not ready");
            else:
                return data["data"];
            
        elif(bitmap_PNG == True):
            data = self.__canvas_data_for_date[date][node-1];
            if data["indicator"] == "not_ready":
                raise NotPresentError("data in this format is not ready");
            else:
                return data["frames"];
        
        elif(aggregated == True):
            data = self.__aggregated_data_for_date[date][node-1];
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
            with open("C:\\Users\\walluser\\javaWorkspace\\D3EventServer\\D3\\WebContent\\wall_coord_data.txt", "r") as read_file:
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
        
        elif("raw" in self.arg):
            #reading the csv files in the memory
            with open(self.path,"r") as f:
                self.data_holder['data']=f.read();
                
            self.data_holder["indicator"]='ready'; 
            
        elif("bitmap" in self.arg):
            #putting the line data into a object to stream
            with open(self.path+"\\data.json","rb")as f:
                self.data_holder['data'] = json.dumps(cPickle.load(f));          
#             with open(self.path+"\\data.json","rb")as f:
#                 output = cPickle.load(f);  
            #not loading images into memory if there is none images
            if(self.data_holder['data']=='""'):
                #indicating that reading in memory is finished for this data  
                self.data_holder['frames']=(0,[]);
                self.data_holder["indicator"]='ready'; 
                return;
#             if(not output):
#                 self.data_holder['data']= msgpack.packb(output,use_bin_type=True);
#                 self.data_holder["indicator"]='ready'; 
#                 return;     
            #just in case there is some data to stream add all the PNGS to a list   
#             output['frames']=[];
            content_length =0; #calculate the content length in bytes of all images to stream in total
            PNGS=[]; #list to hold all the pngs data in memory
            #reading all the images to memory to stream
            for x in xrange(1,31):
                buf_string = cStringIO.StringIO();
                Image.open(self.path+"\\imgs\\"+str(x)+".png").save(buf_string, format="PNG", quality=100);
                content_length = content_length+(buf_string.tell()+4); 
                PNGS.append(struct.pack('>I',buf_string.tell())+buf_string.getvalue());
                buf_string.close();
#             for x in xrange(1,31):
#                 buf_string = cStringIO.StringIO();
#                 Image.open(self.path+"\\imgs\\"+str(x)+".png").save(buf_string, format="PNG", quality=100);
#                 output['frames'].append(buf_string.getvalue());
#                 buf_string.close();
                
            self.data_holder['frames']=(content_length,PNGS);
#             self.data_holder['data']=msgpack.packb(output,use_bin_type=True);
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
        

        
    
    
    
    
    
    
    
    
    
    
