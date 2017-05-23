'''
Created on Feb 29, 2016

@author: Ujjwal Acharya
'''
import os,ast;
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler


# Create custom HTTPRequestHandler class
class customHTTPRequestHandler(BaseHTTPRequestHandler):
    # method for handling the http get request from the client
    def do_GET(self):
        output_directory = "C:/Users/walluser/Desktop/testing";
        try:
            
            file_to_send = open(output_directory + self.path, "r");  # opening the file to send
            #send a connection was made response code
            self.send_response(200,"ok");
            self.send_header("Access-Control-Allow-Origin","null");
            self.end_headers();
            # sending file to client via output stream
            self.wfile.write(file_to_send.read()) 
            self.wfile.close();
            file_to_send.close();
            return;
        except IOError:
            self.send_error(404, 'file not found')
            file_to_send.close()
            return
    
    #method for handling http post request  
    def do_POST(self):
        try:
            content_len = int(self.headers['content-length']);
            posted_message = self.rfile.read(content_len);
            required_data_date = ast.literal_eval(posted_message)["date"];
            #send a connection was made response code
            self.send_response(200,"ok");
            self.send_header("Access-Control-Allow-Origin","null");
            self.end_headers();
            if(os.path.isfile("C:/Users/walluser/Desktop/testing/data"+str(required_data_date)+".csv")):
                self.wfile.write("Ready");
                self.wfile.close();
            else:
                self.wfile.write("Not Ready");
                
                self.wfile.close();   
        except:
            self.send_error(404,"file not found here i was found");
            return;
     

def runServer():
    try:
        server_address = ("127.0.0.1", 8085);
        httpServer = HTTPServer(server_address, customHTTPRequestHandler);
        print("web server is running");
        httpServer.serve_forever();     
    except KeyboardInterrupt:
        print("server is closing");
        httpServer.socket.close();
        

def sendFile():
    try:
        file_path = "//WALL3/Users/walluser/javaWorkspace/D3EventServer/D3/WebContent/dataSetForBarChart.csv";
        file_read = open(file_path, "r");
        for line in file_read:
            print(line);
    except IOError:
        print("the file was not found check again");
        

if __name__ == '__main__':
#     sendFile();
    runServer();
        
    
    
    
