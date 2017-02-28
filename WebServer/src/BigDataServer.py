'''
Created on Feb 29, 2016

@author: Ujjwal Acharya
'''
import os
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

# Create custom HTTPRequestHandler class
class customHTTPRequestHandler(BaseHTTPRequestHandler):
    # method for handling the http request from the client
    def do_GET(self):
        output_directory = "/Users/Uzwal/Documents/workspace/bilevel/WebContent";
        try:
            #printing headers of connection made
            print(self.headers);
            file_to_send = open(output_directory + self.path, "r");  # opening the file to send
            #send a connection was made response code
            self.send_response(200,"ok");
            self.send_header("Access-Control-Allow-Origin","null");
            self.end_headers();
            # sending file to client via output stream
            self.wfile.write(file_to_send.read()) 
            file_to_send.close();
            return;
        except IOError:
            self.send_error(404, 'file not found')
            file_to_send.close()
            return
     

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
    sendFile();
    runServer();
        
    
    
    
