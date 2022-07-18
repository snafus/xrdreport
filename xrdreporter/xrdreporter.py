import argparse 
import json
import logging
import re
import socketserver
import threading
import xml

from threading import Lock
from xml.dom import minidom

from requestHandlers import MyUDPRequestHandler
from observers import FileObserver, LoggerObserver, SummaryLoggerObserver, ElasticSearchObserver


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Process and report output from xrd.report messages')
    parser.add_argument('-p','--port', default=2036, type=int, help='server port number to listen on')
    parser.add_argument('-a','--address', default="127.0.0.1", type=str, help='server listen address')
    parser.add_argument('-d','--debug',help='Enable additional logging',action='store_true')
    parser.add_argument('-l','--log',help='Send all logging to a dedicated file',dest='logfile',default=None)

    # es parameters
    parser.add_argument('-e','--eshosts', default=[], type=str, nargs='+',help='Elastic search hosts')


    args = parser.parse_args()
    logging.basicConfig(level= logging.DEBUG if args.debug else logging.INFO,
                    filename=None if args.logfile is None else args.logfile,
                    format='XRDREPORT-%(asctime)s-%(process)d-%(levelname)s-%(message)s',                  
                    )

    # register observers against the handler. These send the process output elsewhere
    MyUDPRequestHandler.observers.append(SummaryLoggerObserver(logging.DEBUG))
    MyUDPRequestHandler.observers.append(FileObserver("blah.json"))
    MyUDPRequestHandler.observers.append(ElasticSearchObserver(hosts=args.eshosts))


    # prepare and start the loop 
    ServerAddress = (args.address, args.port)
    # Create a Server Instance using context manager
    # Each request is processed through a different thread
    with socketserver.ThreadingUDPServer(ServerAddress, MyUDPRequestHandler) as UDPServerObject:
        # Make the server wait forever serving connections
        UDPServerObject.serve_forever()
    logging.info("Server terminating")
