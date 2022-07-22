#!/usr/bin/env python3

import argparse 
import configparser
import json
import logging
import os
import re
import socketserver
import threading
import xml

from pathlib import Path
from threading import Lock
from xml.dom import minidom

from requestHandlers import MyUDPRequestHandler
from observers import FileObserver, LoggerObserver, SummaryLoggerObserver, ElasticSearchObserver, InfluxDB2Observer


def create_observers(config):
    """create a list of observers, based on input of external config-parser output"""
    import importlib
    module = importlib.import_module('observers')

    observers = []
    for section_name in config.sections():
        section = config[section_name]
        if 'observer' not in section:
            continue # not an observer section
        if section.getboolean('enabled',fallback=False) == False:
            logging.info("Observer {} is disabled; use 'enabled = true' to enable it".format(section_name))
            continue # disabled
        ObserverClass = getattr(module, section.get('observer'))
        #copy the params for the section
        #don't print out params here, in case there's sensitive info
        params = dict(section)
        #create the object
        observers.append( ObserverClass(params) )


    logging.debug('Observers created: {}'.format(observers))
    #raise Exception("Stop")
    return observers



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Process and report output from xrd.report messages')
    parser.add_argument('-c','--config', type=Path, default=None, help='ini config file for Observer connection params, etc.')

    # parser.add_argument('-p','--port', default=2036, type=int, help='server port number to listen on')
    # parser.add_argument('-a','--address', default="127.0.0.1", type=str, help='server listen address')
    parser.add_argument('-d','--debug',help='Enable additional logging',action='store_true')
    parser.add_argument('-l','--log',help='Send all logging to a dedicated file',dest='logfile',default=None)
    parser.add_argument('--deltas',help='Also calculate derivatives between measurements',action='store_true')

    # # es parameters
    # parser.add_argument('-e','--eshosts', default=[], type=str, nargs='+',help='Elastic search hosts')


    args = parser.parse_args()
    logging.basicConfig(level= logging.DEBUG if args.debug else logging.INFO,
                    filename=None if args.logfile is None else args.logfile,
                    format='XRDREPORT-%(asctime)s-%(process)d-%(levelname)s-%(message)s',                  
                    )

    if args.config is not None:
        config = configparser.ConfigParser()
        config.read(args.config)

        logging.debug("Sections: {}".format(','.join(config.sections())))



    MyUDPRequestHandler.do_deltas = args.deltas
    # register observers against the handler. These send the process output elsewhere
    # MyUDPRequestHandler.observers.append(SummaryLoggerObserver({'level':'INFO'}))
    # if args.debug:
    #     MyUDPRequestHandler.observers.append(LoggerObserver({'level':'DEBUG'}))
    # # dynamic loading of any components specified in the config file
    if args.config:
        MyUDPRequestHandler.observers.extend( create_observers(config) )
        MyUDPRequestHandler.include_fields = config['DEFAULT'].get('include_fields',".*")
        MyUDPRequestHandler.exclude_fields = config['DEFAULT'].get('exclude_fields',"")

    logging.debug("Configured Observers: \n\t{}".format( "\n\t".join(str(x) for x in MyUDPRequestHandler.observers)))


    # prepare and start the loop 
    ServerAddress = (config['SERVER'].get('address'), config['SERVER'].getint('port'))
    # Create a Server Instance using context manager
    # Each request is processed through a different thread
    with socketserver.ThreadingUDPServer(ServerAddress, MyUDPRequestHandler) as UDPServerObject:
        # Make the server wait forever serving connections
        UDPServerObject.serve_forever()
    logging.info("Server terminating")
