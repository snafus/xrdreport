import logging
import re
import socketserver
import threading
import xml

from numbers import Number
from xml.dom import minidom

from xrdreporter.observers import Observer
from xrdreporter.xrdLabels import XrdKey



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
                        key = f'{tag_name}__{j.nodeName}__{k.nodeName}'
                        if key == 'cache__rd__hits':
                            # value has extraneous ">" in last character
                            data[key] = to_numeric(re.match('(\d+)', k.firstChild.data).group(1))
                        else:
                            data[key] = to_numeric(k.firstChild.data)
                else:
                    data[f'{tag_name}__{j.nodeName}'] = to_numeric(j.firstChild.data)
    except Exception as e:
        logging.debug("Error string parsing {}".format(dataraw[0:min(len(dataraw),50)]))
        logging.error("Bad xml detected")
        raise e
    return data


def filter_stats(stats: dict, re_includes, re_excludes):
    """filter the stats, based on lists of compiled re expressions"""
    new_stats = {}
    for k,v in stats.items():
        for re_exp in re_excludes:
            if re_exp.match(k) is not None:
                #logging.debug("Excluded on match: {} {}".format(re_exp, k))
                continue # exclude the key
        for re_exp in re_includes:
            if re_exp.match(k):
                new_stats[k] = v
                #logging.debug("Included on match: {} {}".format(re_exp, k))
                continue # passed at least one match
    return new_stats

def augment_raltier1(stats: dict):
    """Specific addtions/changes for the tier-1 configuration"""
    
    new_stats = dict(stats)

    host = stats.get(XrdKey.INFO_HOST, "")
    if type(host) != str:
        logging.error(f'host {XrdKey.INFO_HOST} from {XrdKey.SRC} invalid')
        host = str(stats.get(XrdKey.SRC, "")).split(":")[0]

    
    if 'nubes' in host:
        new_stats['host_type'] = 'VM'
    elif 'lcg' in host:
        # in current setup WN containers have a random hostname
        # this will have no effect
        new_stats['host_type'] = 'WN'
    elif 'ceph-dev' in host:
        new_stats['host_type'] = 'gateway-dev'
    elif 'ceph-' in host:
        new_stats['host_type'] = 'gateway'
    elif 'eos' in host:
        new_stats['host_type'] = 'eos'
    elif 'cta' in host:
        new_stats['host_type'] = 'cta'
    else:
        new_stats['host_type'] = 'unknown'

    # special hack for WN naming
    if (XrdKey.INFO_NAME in stats and stats[XrdKey.INFO_NAME] == 'ceph' and 
                                      stats[XrdKey.INFO_PORT] == 1094 ):
        #Make sure that the proxy instance has the correct label
        new_stats[XrdKey.INFO_NAME] = 'proxy'

    return new_stats


class MyUDPRequestHandler(socketserver.DatagramRequestHandler):
    observers = []
    do_deltas = False
    last_values = dict()
    include_fields = ".*"
    exclude_fields = ""

    def __init__(self,*args,**kwargs):
        self._include_fields = [re.compile(x.strip()) for x in self.include_fields.split(",") if len(x)]
        self._exclude_fields = [re.compile(x.strip()) for x in self.exclude_fields.split(",") if len(x)]
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
        logging.debug("Datagram starts: {}".format(datagram[0:min(len(datagram),20)]))
        if datagram == "ping":
            # self.socket.sendall("pong".encode('utf-8'))
            logging.info("Ping sent from {}".format(self.client_address))
            socket = self.request[1]
            socket.sendto("pong".encode('utf-8'), self.client_address)
            return 

        if len(datagram) == 0:
            logging.debug("Message with no data")
            return

        # convert the xml into a dict
        try:
            stats = parse_dom(datagram)
        except Exception as e:
            raise(e)

        stats = filter_stats(stats, self._include_fields, self._exclude_fields)

        # special hacks for the tier1
        stats = augment_raltier1(stats)

        if self.do_deltas:
            # calculate the deltas, and set stats to new dict
            stats = self._caclulate_deltas(stats)
            # replace the old value
            self.last_values[stats['src']] = stats 

        # pass the dict to all registered observers
        for obs in self.observers:
            obs.serve(stats)
