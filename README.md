# xrdreporter

Python-based tool to collate the UDP packets from the XrootD xrd.report montoring and parse on to another tool for collection.
E.g. use this tool to send data into Elastic search, or, just to collect in a log file

# Basic usage
Starting the program creates a UDP listener, which then reacts to any correctly formatted UDP packet that it receieves. 
Depending on which Observer instances have been registered against the handler, the output is then formatted and sent on to the relevant endpoint.