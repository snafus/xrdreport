import json
import logging
import os
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

from xrdLabels import XrdKey

try:
    import influxdb_client
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
except ImportError:
    pass # slience for now

try:
    import influxdb as influxdbv1
except ImportError:
    pass # slience for now


class Observer():
    # Note Observers are required to be thread-safe
    def serve(self,data: dict):
        pass

class LoggerObserver(Observer):
    def __init__(self, params):
        super().__init__()
        self.level = getattr(logging, params['level'].upper()) # e.g. INFO

    def serve(self, data: dict):
        logging.log(self.level,data)
    def __str__(self):
        return "Logger({})".format(logging.getLevelName(self.level))

class SummaryLoggerObserver(Observer):
    fields = ['src','pgm','ins','link__num','tod','sgen__toe','delta_s','sgen__et','link__in','delta_link__in']
    def __init__(self, params):
        super().__init__()
        self.level = getattr(logging, params['level'].upper()) # e.g. INFO
        self.fields = self.fields if not 'fields' in params else [x.strip() for x in params['fields'].split(",")]

    def serve(self, data: dict):
        d = {k: data.get(k,"N/A") for k in self.fields}
        logging.log(self.level,"{}".format(d))
    def __str__(self):
        return "SummaryLogger({})".format(logging.getLevelName(self.level))


class FileObserver(Observer):
    def __init__(self, params):
        super().__init__()
        self.filename = params['filename']
    def serve(self, data: dict):
        with Lock():
            with open(self.filename,'a') as foo:
                foo.write("{}\n".format(json.dumps(data)))
    def __str__(self):
        return "File(\"{}\")".format(self.filename)


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


class InfluxDB2Observer(Observer):
    def __init__(self, params: dict):
        self.measurement = params['measurement'] # influx measurement name
        self.tags = [XrdKey.INFO_HOST, XrdKey.INFO_PORT, XrdKey.INFO_NAME, 
                XrdKey.SITE, XrdKey.PGM,XrdKey.VER]
        self.excluded = self.tags + [XrdKey.SRC, XrdKey.INS, XrdKey.PID]
        if 'api' in params and params['api'] == 'v1':
            self.api = 'v1'
            self.connection_param = {'host':params['host'],
                                     'port':int(params['port']),
                                     'username':params['username'],
                                     'password':params['password'],
                                     'database':params['database']
                                     }
        else:
            self.api = 'v2'
            self.bucket = params['bucket']
            self.connection_param = { 'token': params['token'] if 'token' in params else  os.environ.get(params['token_env']),
                    'org': params['org'], 
                    'url': params['url'], 
                }

    def _write_data(self, records):
        data =[]
        for item in records:
            v ="{},".format(self.measurement) # measurement
            v += ','.join( "{}={}".format(k,v) for k,v in item['tags'].items() ) # tags
            v += " "
            v += ','.join( "{}={}".format(k,v) for k,v in item['fields'].items() ) # fields
            v += ' {}\n'.format(item['timestamp']) # time
            data.append(v)

        with InfluxDBClient(**self.connection_param) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            write_api.write(bucket=self.bucket, org=self.connection_param['org'], record=data)

    def _write_data_v1(self, records):
        data =[]
        for item in records:
            v = {'measurement': self.measurement, 
                 'tags': item['tags'],
                 'time': item['timestamp'],
                 'fields': item['fields']
            }
        data.append(v)

        # with influxdbv1.InfluxDBClient(**self.connection_param) as client:
        #     client.write_points(data)
        client = influxdbv1.InfluxDBClient(**self.connection_param)
        client.write_points(data)
        client.close()




    def serve(self, data: dict):
        tags = {k:f'{data[k]}' for k in self.tags if k in data}
        tags['reporthost'] = getfqdn()

        fields = {}
        for k,v in data.items():
            if k in self.excluded:
                continue
            fields[k] = f'\"{v}\"' if type(v) == str else v

        timestamp = int(data[XrdKey.TOD]*1e9)

        if self.api == 'v2':
            self._write_data(records=[{'tags':tags,
                                    'timestamp':timestamp,
                                    'fields':fields}])
        else:
            self._write_data_v1(records=[{'tags':tags,
                             'timestamp':timestamp,
                             'fields':fields}])


    def __str__(self):
        if self.api == 'v2':
            return "InfluxDB2({}, {}, {})".format(self.connection_param['url'], self.bucket, self.measurement)
        else:
            return "InfluxDBv1({}, {}, {})".format(self.connection_param['host'], self.connection_param['database'] 
                                                  ,self.measurement)


        
