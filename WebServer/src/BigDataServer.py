'''
Created on Feb 29, 2016

@author: Ujjwal Acharya
'''
import ast,os;
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import SocketServer,urlparse;
from DataCreator import DataCreator,NotPresentError;
from threading import Thread;

class CustomHTTPRequestHandler(BaseHTTPRequestHandler): 
    """ Custom http request handler class which process request and sends response to the client based on the request type"""
    
    def do_GET(self):
        """ method for handling the http get request from the client used for exposing get data api from this server and stream the requested data based on
        date and type """
        try:
            #extracting path and query string from get method
            path, _ ,query_string = self.path.partition('?');
            print(path,query_string);
            queries = urlparse.parse_qs(query_string,strict_parsing=True);
            
            if(queries['op'][0]=='OPEN'):
                
                if ("world-map" in path):
                    file_path = "C:\\Users\\walluser\\javaWorkspace\\D3EventServer\\D3\\WebContent\\world-map.json";             
                    #sending all the required headers
                    self.send_response(200,"ok");
                    self.send_header("Access-Control-Allow-Origin","*");
                    self.send_header('mimetype','application/json');
                    self.send_header("Content-Length",os.path.getsize(file_path));
                    self.send_header('Connection', 'keep-alive');
                    self.end_headers();
                     
                    file_to_send = open(file_path, "rb");  # opening the file to send
                    # sending file to client via output stream
                    self.wfile.write(file_to_send.read()) 
                    self.wfile.flush();
                    
                elif ("data" in path):
                    self._send_data_to_client(queries);
                
            else:
                return;
            
        except IOError:
            self.send_error(404, 'file not found')
            return
        
    def _send_data_to_client(self,query_dict):
        
        date = query_dict['date'][0];
        node = query_dict['node'][0];
        data_type = query_dict['type'][0];
                              
        if(data_type=="BITMAP"):    
            start = self.path.index("_");
            end = self.path.rfind("_");
            date = int(self.path[start+1:end]);
            output = data_creator.get_available_data(date, 0,bitmap=True);  
            #sending all the required headers             
            self.send_response(200,"ok");
            self.send_header('mimetype','multipart/json');
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
         
        elif(data_type=="BITMAP-JSON"):   
             
            output = data_creator.get_available_data(int(date),int(node),bitmap_json=True);
            #sending all the required headers
            self.send_response(200,"ok");
            self.send_header('mimetype','image/png');
            self.send_header("Access-Control-Allow-Origin","null");
            self.send_header("Content-Length",len(output));
            self.send_header('Connection', 'keep-alive');
            self.end_headers();
            
            #streaming the required data to client
            self.wfile.write(output);
            self.wfile.flush();
        
        elif(data_type=="BITMAP-PNG"):   
             
            output = data_creator.get_available_data(int(date),int(node),bitmap_PNG=True);
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
              
        elif(data_type=="AGG"):       
                  
            output = data_creator.get_available_data(int(date),int(node),aggregated=True);
            #sending all the required headers
            self.send_response(200,"ok");
            self.send_header("Access-Control-Allow-Origin","null");
            self.send_header('mimetype','application/json');
            self.send_header("Content-Length",len(output));
            self.send_header('Connection', 'keep-alive');
            self.end_headers();
             
            # sending file to client via output stream
            self.wfile.write(output) 
            self.wfile.flush();
             
        elif(data_type=="RAW"):
                          
            output= data_creator.get_available_data(int(date),int(node),raw=True);
            #sending all the required headers
            self.send_response(200,"ok");
            self.send_header("Access-Control-Allow-Origin","*");
            self.send_header('mimetype','application/json');
            self.send_header("Content-Length",len(output));
            self.send_header('Connection', 'keep-alive');
            self.end_headers();
             
            # sending file to client via output stream
            self.wfile.write(output) ;
            self.wfile.flush();
        else:
            return;

    
    def do_POST(self):
        """method for handling http post request which exposes data check api for checking if the data in certain format 
        and certain date is ready to stream or not """
        
        content_len = int(self.headers['content-length']);
        posted_message = self.rfile.read(content_len);
        print("the data used to check data availability is "+posted_message);
        posted_message = ast.literal_eval(posted_message);
        operation = posted_message['op'];
        
        if(operation=="CHECK"):
            required_data_date = posted_message['date'];
            data_type = posted_message['type'];
    
            if(data_type=="RAW"):              
                try:
                    response = data_creator.check_available_data(required_data_date, raw=True);
                except NotPresentError:
                        response = "not_ready"
            elif(data_type=="BITMAP"):
                try:
                    response = data_creator.check_available_data(required_data_date,bitmap=True);
                except NotPresentError:
                        response = "not_ready" 
            elif(data_type=="AGG"):
                try:
                    response = data_creator.check_available_data(required_data_date,aggregated=True); 
                except NotPresentError:
                        response = "not_ready"
               
            #sending all the required headers to the client
            self.send_response(200,"ok");
            self.send_header("Access-Control-Allow-Origin","*");
            self.send_header('mimetype','text/plain');
            self.send_header("Content-Length",len(response));
            self.send_header('Connection', 'keep-alive');
            self.end_headers();
            #sending the response back to client
            self.wfile.write(response);
            self.wfile.flush();

        else:
            return;
        
class ThreadedServer(SocketServer.ThreadingMixIn,HTTPServer):
    """This class implements multi threaded httpserver so that more than one request can be handled at once"""
    pass;

def runServer():
    """ The http server which fetches data from hdfs and streams to client upon request """
    try:
        server_address = ("10.29.3.2", 8085);
        httpServer = ThreadedServer(server_address, CustomHTTPRequestHandler);
        print("web server is running");
        httpServer.serve_forever();     
    except KeyboardInterrupt:
        print("server is closing");
        httpServer.socket.close();
        
        
if __name__ == '__main__':
    global data_creator; #one object to hold all the data to stream to the client
    #starting the new thread to run server separately
    server = Thread(target=runServer);
    server.start();
    DataCreator.read_transformed_coordinates_to_array();
    #instance that creates data in different format for each date
    data_creator =  DataCreator();   
    data_creator.create_data_for_date(20140130);
    print(data_creator.check_available_data(20140130,aggregated=True))
    print(data_creator.check_available_data(20140130,bitmap=True))
    #continuing with the regular server active process for data creation

    
