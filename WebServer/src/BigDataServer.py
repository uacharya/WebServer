'''
Created on Feb 29, 2016

@author: Ujjwal Acharya
'''
import ast,os;
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import SocketServer;
from DataCreator import DataCreator,NotPresentError;
from threading import Thread;
# Create custom HTTPRequestHandler class
class CustomHTTPRequestHandler(BaseHTTPRequestHandler): 
    """ Custom http request handler which process request and sends response to the client based on the request type"""
    # method for handling the http get request from the client
    def do_GET(self):
        try:
            if("bitmap" in self.path and "PNGS" not in self.path):
                       
                start = self.path.index("_");
                end = self.path.rfind("_");
                date = int(self.path[start+1:end]);
                output = data_creator.get_available_data(date, 0,bitmap=True);  
                #sending all the required headers             
                self.send_response(200,"ok");
                self.send_header('mimetype','multipart/json+png');
                self.send_header("Access-Control-Allow-Origin","null"); 
                self.send_header("Content-Length",len(output));
                self.send_header('Connection', 'keep-alive');
                self.end_headers();
                #streaming the required data to client                
#                 for png in output[1][1]:
#                     self.wfile.write(png);
#                     self.wfile.flush();
                
                self.wfile.write(output);
                self.wfile.flush();
            
            elif("bitmap" in self.path and "PNGS"  in self.path):   
                
                start = self.path.index("_");
                end = self.path.rfind("_");
                date = int(self.path[start+1:end]);
                output = data_creator.get_available_data(date, 0,PNG=True);
                #sending all the required headers
                self.send_response(200,"ok");
                self.send_header('mimetype','image/png');
                self.send_header("Access-Control-Allow-Origin","null");
                self.send_header("Content-Length",output[0]);
                self.send_header('Connection', 'keep-alive');
                self.end_headers();
                #streaming the required data to client
                for img in output[1]:
                    self.wfile.write(img);
                    self.wfile.flush();
                 
            elif("aggregated" in self.path):
                
                start = self.path.index("_");
                end = self.path.rfind("_");
                date = int(self.path[start+1:end]);
                file_path= data_creator.get_available_data(date,0,aggregated=True);
                #sending all the required headers
                self.send_response(200,"ok");
                self.send_header("Access-Control-Allow-Origin","null");
                self.send_header('mimetype','application/json');
                self.send_header("Content-Length",os.path.getsize(file_path));
                self.send_header('Connection', 'keep-alive');
                self.end_headers();
                
                file_to_send = open(file_path, "rb");  # opening the file to send
                # sending file to client via output stream
                self.wfile.write(file_to_send.read()) 
                self.wfile.flush();
                
            elif("raw" in self.path): 
                                  
                start = self.path.index("_");
                end = self.path.rfind("_");
                date = int(self.path[start+1:end]);
                file_path= data_creator.get_available_data(date,0,raw=True);
                #sending all the required headers
                self.send_response(200,"ok");
                self.send_header("Access-Control-Allow-Origin","null");
                self.send_header('mimetype','application/json');
                self.send_header("Content-Length",os.path.getsize(file_path));
                self.send_header('Connection', 'keep-alive');
                self.end_headers();
                
                file_to_send = open(file_path, "rb");  # opening the file to send
                # sending file to client via output stream
                self.wfile.write(file_to_send.read()) 
                self.wfile.flush();
                
            elif ("world-map" in self.path):
                
                file_path = "C:\\Users\\walluser\\Desktop\\testing\\world-map.json";             
                #sending all the required headers
                self.send_response(200,"ok");
                self.send_header("Access-Control-Allow-Origin","null");
                self.send_header('mimetype','application/json');
                self.send_header("Content-Length",os.path.getsize(file_path));
                self.send_header('Connection', 'keep-alive');
                self.end_headers();
                
                file_to_send = open(file_path, "rb");  # opening the file to send
                # sending file to client via output stream
                self.wfile.write(file_to_send.read()) 
                self.wfile.flush();
            return;
        
        except IOError:
            self.send_error(404, 'file not found')
            return
    
    #method for handling http post request  
    def do_POST(self):
        content_len = int(self.headers['content-length']);
        posted_message = self.rfile.read(content_len);
        posted_message = ast.literal_eval(posted_message);
        required_data_date = posted_message["date"];
        data_type = posted_message['type'];

        if(data_type=="raw"):              
            try:
                response = data_creator.check_available_data(required_data_date, raw=True);
            except NotPresentError:
                    response = "not_ready"
        elif(data_type=="bitmap"):
            try:
                response = data_creator.check_available_data(required_data_date,bitmap=True);
            except NotPresentError:
                    response = "not_ready" 
        elif(data_type=="aggregated"):
            try:
                response = data_creator.check_available_data(required_data_date,aggregated=True); 
            except NotPresentError:
                    response = "not_ready"
        #sending all the required headers to the client
        self.send_response(200,"ok");
        self.send_header("Access-Control-Allow-Origin","null");
        self.send_header('mimetype','text/plain');
        self.send_header("Content-Length",len(response));
        self.send_header('Connection', 'keep-alive');
        self.end_headers();
        #sending the response back to client
        self.wfile.write(response);
        self.wfile.flush();

class ThreadedServer(SocketServer.ThreadingMixIn,HTTPServer):
    """This class implements multi threaded httpserver so that more than one request can be handled at once"""
    pass;

def runServer():
    """ The http server which fetches data from hdfs and streams to client upon request """
    try:
        server_address = ("127.0.0.1", 8085);
        httpServer = ThreadedServer(server_address, CustomHTTPRequestHandler);
        print("web server is running");
        httpServer.serve_forever();     
    except KeyboardInterrupt:
        print("server is closing");
        httpServer.socket.close();
        
        
if __name__ == '__main__':
    global data_creator; #one object to hold all the data to stream to the client
    from datetime import datetime;
    start = datetime.now().strftime("%H%M%S%f");
    #starting the new thread to run server separately
#     server = Thread(target=runServer);
#     server.start();
    DataCreator.read_transformed_coordinates_to_array();
    #instance that creates data in different format for each date
    data_creator =  DataCreator();   
    data_creator.create_data_for_date(1929,aggregation_width=10);
    print(data_creator.check_available_data(1929,aggregated=True))
    #continuing with the regular server active process for data creation
    end = datetime.now().strftime("%H%M%S%f");
    print(end,start);
    print(float(end)-float(start));
    
