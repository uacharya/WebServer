'''
Created on Feb 29, 2016

@author: Ujjwal Acharya
'''
import ast;
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from DataCreator import DataCreator,NotPresentError;
from threading import Thread;


# Create custom HTTPRequestHandler class
class customHTTPRequestHandler(BaseHTTPRequestHandler):
    """ Custom http request handler which process request and sends response to the client based on the request type"""
    # method for handling the http get request from the client
    def do_GET(self):
        try:
            if("bitmap" in self.path and "PNGS" not in self.path):
                #sending all the required headers
                self.send_response(200,"ok");
                self.send_header('mimetype','application/json');
                self.send_header("Access-Control-Allow-Origin","null"); 
                self.end_headers();
                
                start = self.path.index("_");
                end = self.path.rfind("_");
                date = int(self.path[start+1:end]);
                output = data_creator.get_available_data(date, 0,bitmap=True);
                #streaming the required data to client
                self.wfile.write(output);
                self.wfile.flush();
                self.wfile.close();
            
            elif("bitmap" in self.path and "PNGS"  in self.path):   
                #sending all the required headers
                self.send_response(200,"ok");
                self.send_header('mimetype','image/png');
                self.send_header("Access-Control-Allow-Origin","null");
                self.end_headers();
                
                start = self.path.index("_");
                end = self.path.rfind("_");
                date = int(self.path[start+1:end]);
                output = data_creator.get_available_data(date, 0,PNG=True);
                #streaming the required data to client
                for img in output:
                    self.wfile.write(img);
                    self.wfile.flush();
                self.wfile.close();
                 
            elif("aggregated" in self.path):
                #sending all the required headers
                self.send_response(200,"ok");
                self.send_header("Access-Control-Allow-Origin","null");
                self.end_headers();
                
                start = self.path.index("_");
                end = self.path.rfind("_");
                date = int(self.path[start+1:end]);
                file_path= data_creator.get_available_data(date,0,aggregated=True);
                
                file_to_send = open(file_path, "rb");  # opening the file to send
                # sending file to client via output stream
                self.wfile.write(file_to_send.read()) 
                self.wfile.flush();
                self.wfile.close();
                
            elif("raw" in self.path):   
                #sending all the required headers
                self.send_response(200,"ok");
                self.send_header("Access-Control-Allow-Origin","null");
                self.end_headers();
                
                start = self.path.index("_");
                end = self.path.rfind("_");
                date = int(self.path[start+1:end]);
                file_path= data_creator.get_available_data(date,0,raw=True);
                
                file_to_send = open(file_path, "rb");  # opening the file to send
                # sending file to client via output stream
                self.wfile.write(file_to_send.read()) 
                self.wfile.flush();
                self.wfile.close();
                
            elif ("world-map" in self.path):
                #sending all the required headers
                self.send_response(200,"ok");
                self.send_header("Access-Control-Allow-Origin","null");
                self.end_headers();
                
                file_to_send = open("/Users/Uzwal/Desktop/ineGraph/world-map.json", "rb");  # opening the file to send
                # sending file to client via output stream
                self.wfile.write(file_to_send.read()) 
                self.wfile.flush();
                self.wfile.close();

            return;
        
        except IOError:
            self.send_error(404, 'file not found')
            return
    
    #method for handling http post request  
    def do_POST(self):
        
        content_len = int(self.headers['content-length']);
        posted_message = self.rfile.read(content_len);
        required_data_date = ast.literal_eval(posted_message)["date"];
        data_type = ast.literal_eval(posted_message)['type'];
    
        #send a connection was made response code
        self.send_response(200,"ok");
        self.send_header("Access-Control-Allow-Origin","null");
        self.end_headers();
        
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
    
        #sending the response back to client
        self.wfile.write(response);
        self.wfile.flush();
        self.wfile.close();
                   

def runServer():
    """ The http server which fetches data from hdfs and streams to client upon request """
    try:
        server_address = ("127.0.0.1", 8085);
        httpServer = HTTPServer(server_address, customHTTPRequestHandler);
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
    #instance that creates data in different format for each date
    data_creator =  DataCreator();
    data_creator.create_data_for_date(1929,aggregation_width=10);
    #continuing with the regular server active process for data creation



    
        
    
    
    
