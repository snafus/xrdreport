import logging
import re
import socketserver
import threading
import xml

from xml.dom import minidom

from observers import Observer


#https://github.com/xrootd/xrootd/blob/c3afc27d72c9b0460bfac05967d6a7ea8f9fa99a/src/XrdApps/XrdMpxXml.cc
name_mappings="""{"src",             "Server location:"},
{"tod",             "~Statistics:"},
{"tos",             "~Server started: "},
{"pgm",             "Server program: "},
{"ins",             "Server instance:"},
{"pid",             "Server process: "},
{"site",            "Server sitename: "},
{"ver",             "Server version: "},
{"info.host",       "Host name:"},
{"info.port",       "Port:"},
{"info.name",       "Instance name:"},
{"buff.reqs",       "Buffer requests:"},
{"buff.mem",        "Buffer bytes:"},
{"buff.buffs",      "Buffer count:"},
{"buff.adj",        "Buffer adjustments:"},
{"buff.xlreqs",     "Buffer XL requests:"},
{"buff.xlmem",      "Buffer XL bytes:"},
{"buff.xlbuffs",    "Buffer XL count:"},
{"link.num",        "Current connections:"},
{"link.maxn",       "Maximum connections:"},
{"link.tot",        "Overall connections:"},
{"link.in",         "Bytes received:"},
{"link.out",        "Bytes sent:"},
{"link.ctime",      "Total connect seconds:"},
{"link.tmo",        "Read request timeouts:"},
{"link.stall",      "Number of partial reads:"},
{"link.sfps",       "Number of partial sends:"},
{"poll.att",        "Poll sockets:"},
{"poll.en",         "Poll enables:"},
{"poll.ev",         "Poll events: "},
{"poll.int",        "Poll events unsolicited:"},
{"proc.usr.s",      "Seconds user time:"},
{"proc.usr.u",      "Micros  user time:"},
{"proc.sys.s",      "Seconds sys  time:"},
{"proc.sys.u",      "Micros  sys  time:"},
{"xrootd.num",      "XRootD protocol loads:"},
{"xrootd.ops.open", "XRootD opens:"},
{"xrootd.ops.rf",   "XRootD cache refreshes:"},
{"xrootd.ops.rd",   "XRootD reads:"},
{"xrootd.ops.pr",   "XRootD preads:"},
{"xrootd.ops.rv",   "XRootD readv's:"},
{"xrootd.ops.rs",   "XRootD readv segments:"},
{"xrootd.ops.wr",   "XRootD writes:"},
{"xrootd.ops.sync", "XRootD syncs:"},
{"xrootd.ops.getf", "XRootD getfiles:"},
{"xrootd.ops.putf", "XRootD putfiles:"},
{"xrootd.ops.misc", "XRootD misc requests:"},
{"xrootd.sig.ok",   "XRootD ok  signatures:"},
{"xrootd.sig.bad",  "XRootD bad signatures:"},
{"xrootd.sig.ign",  "XRootD ign signatures:"},
{"xrootd.aio.num",  "XRootD aio requests:"},
{"xrootd.aio.max",  "XRootD aio max requests:"},
{"xrootd.aio.rej",  "XRootD aio rejections:"},
{"xrootd.err",      "XRootD request failures:"},
{"xrootd.rdr",      "XRootD request redirects:"},
{"xrootd.dly",      "XRootD request delays:"},
{"xrootd.lgn.num",  "XRootD login total count:"},
{"xrootd.lgn.af",   "XRootD login auths bad:  "},
{"xrootd.lgn.au",   "XRootD login auths good: "},
{"xrootd.lgn.ua",   "XRootD login auths none: "},
{"ofs.role",        "Server role:"},
{"ofs.opr",         "Ofs reads:"},
{"ofs.opw",         "Ofs writes:"},
{"ofs.opp",         "POSC files now open:"},
{"ofs.ups",         "POSC files deleted:"},
{"ofs.han",         "Ofs handles:"},
{"ofs.rdr",         "Ofs redirects:"},
{"ofs.bxq",         "Ofs background tasks:"},
{"ofs.rep",         "Ofs callbacks:"},
{"ofs.err",         "Ofs errors:"},
{"ofs.dly",         "Ofs delays:"},
{"ofs.sok",         "Ofs ok  events:"},
{"ofs.ser",         "Ofs bad events:"},
{"ofs.tpc.grnt",    "TPC grants:"},
{"ofs.tpc.deny",    "TPC denials:"},
{"ofs.tpc.err",     "TPC errors:"},
{"ofs.tpc.exp",     "TPC expires:"},
{"oss.paths",       "Oss exports:"},
{"oss.space",       "Oss space:"},
{"sched.jobs",      "Tasks scheduled: "},
{"sched.inq",       "Tasks now queued:"},
{"sched.maxinq",    "Max tasks queued:"},
{"sched.threads",   "Threads in pool:"},
{"sched.idle",      "Threads idling: "},
{"sched.tcr",       "Threads created:"},
{"sched.tde",       "Threads deleted:"},
{"sched.tlimr",     "Threads unavail:"},
{"sgen.as",         "Unsynchronized stats:"},
{"sgen.et",         "Mills to collect stats:"},
{"sgen.toe",        "~Time when stats collected:"},
{"ssi.err",         "SSI errors:"},
{"ssi.req.bytes",   "Request total bytes:"},
{"ssi.req.maxsz",   "Request largest size:"},
{"ssi.req.ab",      "Request aborts:"},
{"ssi.req.al",      "Request alerts:"},
{"ssi.req.bnd",     "Requests now bound:"},
{"ssi.req.can",     "Requests cancelled:"},
{"ssi.req.cnt",     "Request total count:"},
{"ssi.req.fin",     "Requests finished:"},
{"ssi.req.finf",    "Requests forced off:"},
{"ssi.req.gets",    "Request retrieved:"},
{"ssi.req.perr",    "Request prep errors:"},
{"ssi.req.proc",    "Requests started:"},
{"ssi.req.rdr",     "Requests redirected:"},
{"ssi.req.relb",    "Request buff releases:"},
{"ssi.req.dly",     "Requests delayed:"},
{"ssi.rsp.bad",     "Response violations:"},
{"ssi.rsp.cbk",     "Response callbacks:"},
{"ssi.rsp.data",    "Responses as data:"},
{"ssi.rsp.errs",    "Responses as errors:"},
{"ssi.rsp.file",    "Responses as files:"},
{"ssi.rsp.rdy",     "Responses without delay:"},
{"ssi.rsp.str",     "Responses as streams:"},
{"ssi.rsp.unr",     "Responses with delay:"},
{"ssi.rsp.mdb",     "Response metadata bytes:"},
{"ssi.res.add",     "Resources added:"},
{"ssi.res.rem",     "Resources removed:"}
{"xrootd.ops.wv",   "XRootD writev's:"},
{"xrootd.ops.ws",   "XRootD writev's segments:"},
""".strip().split('\n')
label_map = {}
for line in name_mappings:
    reg = re.search('\"(.*?)\".*"(.*?)".*',line)
    label_map[reg[1].replace(".",'_')] = reg[2].replace(":",'')

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

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        logging.debug("Observers: {}".format(self.observers))

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

        # pass the dict to all registered observers
        for obs in self.observers:
            obs.serve(stats)