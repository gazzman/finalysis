finalysis
=========

A Python repo for analyzing financial data.


data_collection.parse_option_chain.py
-------------------------------------

A module for parsing an xml document containing option chain info and storing
it in a posgresql database. A ChainParser object is instantiated with a the 
name of a file containing the data and the name of the database. This should be
all you need if you use a .pgpass file. Otherwise, the optional dbhost arg can
be passed to ChainParser as a string in the format used by sqlalchemy:
    
    user:password@host:port

The example.xml file is an example of the file format used by the ChainParser.


pcp_analysis.analyze_pcp.py
---------------------------

A module for analyzing the Put-Call parity (PCP) relationship. The PCPAnalyzer 
class contains methods for this purpose.


pcp_analysis.pcp_analyzer_server.py
-----------------------------------

This module implements data_collection.parse_option_chain.py and 
pcp_analysis.analyze_pcp.py. It starts a TCP socket server that listens for
messages. The message is the name of the xml file to parse. Once received, the
server forks a new process that initiates a ChainParser object. Once the 
ChainParser object has finished parsing and storing the data, a PCPAnalyzer
object is instantiated. The PCPAnalyzer object returns its results to the
server, which, under certain conditions, will initiate a trade through the
reversal_server.


pcp_analysis.process_one_file.py
--------------------------------

As of now, the server will fork as many processes as it can for concurrencl.
But if you have many files to process at the same time, this can cause 
problems. The process_one_file.py script tries to mitigate this by sleeping 
each time it sends a filename to the server. The length of time it sleeps 
depends on the size of the file to be parsed and the number of 
pcp_analyzer_server processes already forked.

The following is a simple bash script designed to obtain the information 
required by the script:

process_file.bash:

    #!/bin/bash
    for f in `ls $1`
    do
        conproc=-1
        for p in `pgrep pcp_analyzer`
        do
            conproc=$(($conproc + 1))
        done
        size=`ls -l $f | awk '{print $5}'`
        echo $f, $conproc, $size
        process_one_file.py localhost $2 $f $conproc $size
    done
