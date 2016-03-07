'''
Created on Feb 29, 2016

@author: Uzwal
'''
import os
import SimpleHTTPServer
import SocketServer


def runServer():
    try:
        Handler = SimpleHTTPServer.SimpleHTTPRequestHandler # http request and response handler
        httpServer = SocketServer.TCPServer(("127.0.0.1", 8080), Handler)
        print("web server is running")
        httpServer.serve_forever()
    except KeyboardInterrupt:
        print("server is closing")
        httpServer.socket.close();
        

if __name__ == '__main__':
    runServer()
    
