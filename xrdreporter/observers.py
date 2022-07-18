import json
import logging
import re
import requests
import socketserver
import threading
import urllib3

from datetime import date,datetime
from random import choice 
from requests.exceptions import Timeout
from socket import getfqdn
from threading import Lock
from typing import List

class Observer():
    def serve(self,data: dict):
        pass

class LoggerObserver(Observer):
    def __init__(self, level=logging.INFO):
        super().__init__()
        self.level = level

    def serve(self, data: dict):
        logging.log(self.level,data)

class SummaryLoggerObserver(Observer):
    fields = ['src','pgm','ins','link__num']
    def __init__(self, level=logging.INFO):
        super().__init__()
        self.level = level

    def serve(self, data: dict):
        d = {k: data.get(k,"N/A") for k in self.fields}
        logging.log(self.level,"{}".format(d))


class FileObserver(Observer):
    def __init__(self, filename):
        super().__init__()
        self.filename = filename
    def serve(self, data: dict):
        with Lock():
            with open(self.filename,'a') as foo:
                foo.write("{}\n".format(json.dumps(data)))


class ElasticSearchObserver(Observer):
    def __init__(self, hosts: List[str], type_name: str ='echo_xrdrpt'):
        super().__init__()
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.type_name = type_name
        self.hosts = hosts if len(hosts) else ["localhost:1232"]
        self.timeout = 2

    def _url(self):
        es_host = choice(self.hosts)
        day = date.today().strftime("%Y.%m.%d")
        path = f'/logstash-{day}/doc/'
        url = f'{es_host}/{path}'
        return url

    def _prep_request(self, data: dict):
        """Build the data to be sent"""
        
        #make a copy
        params = dict(data)
        # add any extra variables 
        params['reporthost'] = getfqdn()

        # add the type name as prefix to all keys 
        params_new = {}
        for k,v in params.items():
            params_new[f'{self.type_name}_{k}'] = v 
        # do not forget to add the type
        params_new['type'] = self.type_name

        #add some additional parameters
        #Try to makesure get timezone/dst setting based on machine
        params_new['@timestamp'] = datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")

        return params_new

    def serve(self, data: dict):
        url = self._url()

        new_data = self._prep_request(data)

        logging.debug(f'Sending to ES: {new_data}, {url}')
        try:
            logging.info(f'{url} {new_data}')
            #req = requests.post(url=url, verify=False,
            #            json=new_data, timeout=self.timeout)
            #req.raise_for_status()
            #logging.debug(f'ES data result {req.status_code}' )
        except Timeout:
            logging.warning("ES data submission hit timeout")
