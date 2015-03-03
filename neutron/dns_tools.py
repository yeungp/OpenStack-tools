#! /usr/bin/python
#
# File:     dns_tools.py
# Brief:    Utilities for DNS.
#
# Copyright (c) 2015, Cisco Systems

import logging
import logging.handlers as handlers
import sys
import os
import time
from datetime import datetime
from optparse import OptionParser
# from matplotlib import pyplot, dates
import re
import pdb

""" ===========================================================
Configurations
=========================================================== """

LOG_DIR = '/var/log/'
LOG_FILE = LOG_DIR + 'dns_tools.log'

PID_DIR = '/var/lib/neutron/dhcp/'
PID_FILE = 'pid'

FILE_DEFAULT = '/var/log/syslog'
TIMESTAMP_DEFAULT = 0
SAMPLE_DEFAULT = 288
INTERVAL_DEFAULT = 5

""" ===========================================================
Collect samples
=========================================================== """


class Collect(object):
    def __init__(self, sample=SAMPLE_DEFAULT, interval=INTERVAL_DEFAULT):
        self.sample = sample
        self.interval = interval
        self.pids = []
        logger.info("%s:%s() %d: %d samples at %d minutes interval",
                    self.__class__.__name__,
                    sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                    self.sample, self.interval)

    def run(self):
        """
        Collect data for extended time for all dnsmasq.
        """
        logger.info("%s:%s() %d: starting at time stamp %d",
                    self.__class__.__name__,
                    sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                    int(time.time()))
        for i in range(0, self.sample):
            self.get_pids()
            if len(self.pids) > 0:
                self.dump_dns_cache()
            time.sleep(self.interval * 60)

    def get_pids(self):
        """
        Read PID from the pid file under the DHCP directory.
        Populate self.pids
            list of PID for dnsmasq
        """
        self.pids = []
        if os.path.isdir(PID_DIR) is False:
            logger.error("%s:%s() %d: invalid %s", self.__class__.__name__,
                         sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                         PID_DIR)
            return
        dirs = os.listdir(PID_DIR)
        for name in dirs:
            path = os.path.join(PID_DIR, name)
            pid_file = os.path.join(path, PID_FILE)
            with open(pid_file) as f:
                try:
                    lines = f.readlines()
                    pid = int(lines[0].rstrip('\n'))
                    self.pids.append(pid)
                except:
                    logger.warning("%s:%s() %d: ignore %s", self.__class__.__name__,
                                   sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                                   pid_file)
        logger.debug("%s:%s() %d: found %d dnsmasq", self.__class__.__name__,
                     sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                     len(self.pids))

    def dump_dns_cache(self):
        """
        Dump DNS cache for each dnsmasq
        """
        start = time.time()
        for pid in self.pids:
            cmd = "sudo kill -s SIGUSR1 %d" % pid
            os.system(cmd)
        duration = time.time() - start
        logger.info("%s:%s() %d: dumped cache for %d dnsmasq in %d seconds", self.__class__.__name__,
                    sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                    len(self.pids), duration)

    def help(self):
        print("dns_tools.py collect -s S -i I")
        print("    Dump DNS cache to syslog for all dnsmasq S times every I minutes.")

""" ===========================================================
Extract data
=========================================================== """


class Extract(object):
    def __init__(self, log_file=FILE_DEFAULT, ts=TIMESTAMP_DEFAULT, interval=INTERVAL_DEFAULT):
        self.log_file = log_file
        self.ts = ts
        self.interval = interval
        self.data = {}
        self.buckets = {}
        self.x_dates = []
        self.y_queries = []
        self.first_dt = None
        self.last_dt = None
        logger.info("%s:%s() %d: process file %s, starting from timestamp %d, at %d minutes interval",
                    self.__class__.__name__,
                    sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                    self.log_file, self.ts, self.interval)

    def run(self):
        """
        Extract data from collected dnsmasq cache.
        Example of a DNS cache dump
            Feb 26 17:33:49 svl6-csl-b-net-001 dnsmasq[6336]: time 1424972029
            Feb 26 17:33:49 svl6-csl-b-net-001 dnsmasq[6336]: cache size 150, 0/0 cache insertions re-used unexpired cache entries.
            Feb 26 17:33:49 svl6-csl-b-net-001 dnsmasq[6336]: queries forwarded 0, queries answered locally 0
            Feb 26 17:33:49 svl6-csl-b-net-001 dnsmasq[6336]: server 171.70.168.183#53: queries sent 0, retried or failed 0
            Feb 26 17:33:49 svl6-csl-b-net-001 dnsmasq[6336]: server 173.36.131.10#53: queries sent 0, retried or failed 0
            Feb 26 17:33:49 svl6-csl-b-net-001 dnsmasq[6336]: server 173.37.87.157#53: queries sent 0, retried or failed 0
        """
        self.get_data()
        if len(self.data) > 0:
            self.show_data()
            self.show_graph()
        else:
            print("No data found!")

    def get_data(self):
        """
        Populate self.data
            dictionary key      pid of dnsmasq
            dictionary value    dictionary of
                                key = timestamp
                                value = number of forwarded + answered queries
        """
        if os.path.isfile(self.log_file) is False:
            logger.error("%s:%s() %d: invalid %s", self.__class__.__name__,
                         sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                         self.log_file)
            return
        ts = 0
        start = time.time()
        with open(self.log_file) as f:
            for line in f:
                try:
                    m1 = re.search("dnsmasq\[\d+]", line)
                    if m1:
                        words = line.split()
                        p1 = words[4].split('[')
                        p2 = p1[-1].split(']')
                        pid = int(p2[0])
                        if pid not in self.data:
                            self.data[pid] = {}
                        m2 = re.search("dnsmasq\[\d+]: time ", line)
                        if m2:
                            ts = int(words[-1])
                            if ts not in self.data[pid]:
                                self.data[pid][ts] = 0
                        elif ts != 0:
                            m3 = re.search("dnsmasq\[\d+]: queries forwarded ", line)
                            if m3:
                                self.data[pid][ts] = int(words[7].rstrip(',')) + int(words[-1])
                                ts = 0
                except:
                    logger.warning("%s:%s() %d: failed to process '%s'",
                                   self.__class__.__name__,
                                   sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                                   line)
        duration = time.time() - start
        logger.info("%s:%s() %d: processed %d dnsmasq cache records in %d seconds", self.__class__.__name__,
                    sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                    len(self.data), duration)

    def show_data(self):
        """
        Put data from each dnsmasq into buckets.
        Then display histogram.
        buckets     key = ending timestamp of bucket
                    value = number of queries
        """
        print("\n%-20s  %s" % ("Ending Date/Time", "DNS Queries"))
        print("%-20s  %s" % ("----------------", "-----------"))
        period = self.interval * 60
        total = 0
        for pid in self.data:
            last_ts = 0
            last_count = 0
            for ts in sorted(self.data[pid]):
                tm = ts / period * period
                if last_ts != 0:
                    if tm not in self.buckets:
                        self.buckets[tm] = 0
                    self.buckets[tm] = self.buckets[tm] + self.data[pid][ts] - last_count
                    total = total + self.data[pid][ts] - last_count
                    last_tm = tm
                else:
                    first_tm = tm
                last_ts = ts
                last_count = self.data[pid][ts]
        for ts in sorted(self.buckets):
            dts = datetime.fromtimestamp(ts)
            self.x_dates.append(dates.date2num(dts))
            self.y_queries.append(self.buckets[ts])
            dt = dts.strftime("%Y-%m-%d %H:%M:%S")
            print("%-20s  %d" % (dt, self.buckets[ts]))
        self.first_dt = datetime.fromtimestamp(first_tm).strftime("%Y-%m-%d %H:%M:%S")
        self.last_dt = datetime.fromtimestamp(last_tm).strftime("%Y-%m-%d %H:%M:%S")
        print("\n%d queries from %s to %s" % (total, self.first_dt, self.last_dt))

    def show_graph(self):
        fmt = dates.DateFormatter('%m/%d %H:%M')
        fig = pyplot.figure()
        ax = fig.add_subplot(111)
        ax.vlines(self.x_dates, 0, self.y_queries, color='k', linestyle='solid')
        ax.xaxis.set_major_locator(dates.HourLocator())
        ax.xaxis.set_major_formatter(fmt)
        ax.set_ylim(bottom=0)
        text = "DNS queries received by all dnsmasq (%s)" % (self.log_file)
        ax.set_title(text)
        pyplot.ylabel('Queries')
        pyplot.xticks(rotation='vertical')
        pyplot.subplots_adjust(bottom=.3)
        pyplot.show()

    def help(self):
        print("dns_tools.py extract -f F -t T -i I")
        print("    Extract DNS queries from file F since timestamp T and show histogram every I minutes.")

""" ===========================================================
Logging
=========================================================== """
logger = logging.getLogger('dns_tools')


def do_logging():
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=10485760, backupCount=3)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    handler.setFormatter(formatter)

""" ===========================================================
Option Parsing
=========================================================== """
MIN_ARGS = 1
COLLECT_ARGS = 3
EXTRACT_ARGS = 4


def do_parsing():
    file_help = "Text file with output of dnsmasq cache dump. Default is %s." % FILE_DEFAULT
    help_help = "Show this help message and exit."
    interval_help = "Interval to collect samples in minutes. Default is every %s minutes." % INTERVAL_DEFAULT
    sample_help = "Number of samples to collect. Default is %s." % SAMPLE_DEFAULT
    timestamp_help = "Extract data starting from specified time stamp.  Default is %s seconds." % TIMESTAMP_DEFAULT

    parser = OptionParser(add_help_option=False)
    parser.add_option('-f', '--file', action='store', dest='log_file',
                      help=file_help, metavar='FILE', default=FILE_DEFAULT)
    parser.add_option('-h', '--help', action='store_true', dest='help',
                      help=help_help, metavar='HELP')
    parser.add_option('-i', '--interval', action='store', dest='interval',
                      help=interval_help, metavar='INTERVAL', default=INTERVAL_DEFAULT)
    parser.add_option('-s', '--sample', action='store', dest='sample',
                      help=sample_help, metavar='SAMPLE', default=SAMPLE_DEFAULT)
    parser.add_option('-t', '--timestamp', action='store', dest='timestamp',
                      help=timestamp_help, metavar='TIMESTAMP', default=TIMESTAMP_DEFAULT)

    def usage():
        parser.print_help()
        print "\nSupported Operations:"
        print "  dns_tools.py collect [ -h ] [ -s <value> ] [ -i <value> ]"
        print "  dns_tools.py extract [ -h ] [ -f <name> ] [ -t <value> ] [ -i <value ]"
        sys.exit()

    flags, args = parser.parse_args()
    if flags.help and len(args) == 0:
        usage()

    if len(args) == 0 or (args[0] != 'collect' and args[0] != 'extract'):
        logger.error("%s() %d: invalid options!",
                     __name__, sys._getframe().f_lineno)
        print("\nInvalid options!\n")
        usage()

    if args[0] == 'collect':
        if len(args) > COLLECT_ARGS:
            logger.error("%s() %d: too many arguments!",
                         __name__, sys._getframe().f_lineno)
            print("\nToo many arguments!\n")
            usage()
        try:
            collect = Collect(int(flags.sample), int(flags.interval))
            if flags.help:
                collect.help()
            else:
                collect.run()
        except:
            pass
    elif args[0] == 'extract':
        if len(args) > EXTRACT_ARGS:
            logger.error("%s() %d: too many arguments!",
                         __name__, sys._getframe().f_lineno)
            print("\nToo many arguments!\n")
            usage()
        try:
            extract = Extract(flags.log_file, int(flags.timestamp), int(flags.interval))
            if flags.help:
                extract.help()
            else:
                extract.run()
        except:
            pass

""" ===========================================================
Main Program.
=========================================================== """

if __name__ == "__main__":
    do_logging()
    do_parsing()
