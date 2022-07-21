import logging
import re
import socketserver
import threading
import xml

from numbers import Number
from xml.dom import minidom

from observers import Observer



def to_numeric(data: str):
    """If possible convert to numeric type"""
    try:
        d = float(data)
        if d.is_integer():
            return int(d)
        return d
    except ValueError:
        return data

def parse_dom(dataraw):
    try:
        docs = minidom.parseString(dataraw)
        # get general stats items, and put into the final dict
        data = dict((k,to_numeric(v)) for k,v in docs.firstChild.attributes.items())
        stats = docs.getElementsByTagName("stats")
        # now loop over the various components
        for i in stats:
            tag_name = i.getAttribute("id")
            for j in i.childNodes:
                if type(j.firstChild) == xml.dom.minidom.Element:
                    #print (j, j.childNodes)
                    for k in j.childNodes:
                        data[f'{tag_name}__{j.nodeName}__{k.nodeName}'] = to_numeric(k.firstChild.data)
                else:
                    data[f'{tag_name}__{j.nodeName}'] = to_numeric(j.firstChild.data)
    except Exception as e:
        print(docs)
        raise e
    return data



class MyUDPRequestHandler(socketserver.DatagramRequestHandler):
    observers = []
    do_deltas = False
    last_values = dict()

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)

    def _caclulate_deltas(self, stats: dict):
        """Determine differences from previous values, if existing"""
        src = stats.get('src',None)
        if src is None:
            raise ValueError("Missing 'src' key")

        if not src in self.last_values:
            # no previous value, so return
            return stats

        # get the last values, and calculate differences
        last = self.last_values[src]
        new_stats = dict(stats)
        delta_s = float(stats['tod'] - last['tod'])
        new_stats['delta_s'] = delta_s
        if delta_s == 0:
            # set value to 1 to avoid /0 errors
            delta_s = 1.
        for k,v in stats.items():
            if not isinstance(v,Number):
                continue
            new_stats[f'delta_{k}'] = (stats[k] - last[k]) / delta_s

        return new_stats
        


    # Override the handle() method
    def handle(self):
        # Receive and print the datagram received from client
        # Print the name of the thread
        logging.debug("Thread Name:{}; Recieved one request from {}"\
                     .format(threading.current_thread().name,
                             self.client_address[0]))

        datagram = self.rfile.readline().decode('utf_8').strip()

        # convert the xml into a dict
        try:
            stats = parse_dom(datagram)
        except Exception as e:
            print (e)
            raise(e)

        if self.do_deltas:
            # calculate the deltas, and set stats to new dict
            stats = self._caclulate_deltas(stats)
            # replace the old value
            self.last_values[stats['src']] = stats 

        # pass the dict to all registered observers
        for obs in self.observers:
            obs.serve(stats)