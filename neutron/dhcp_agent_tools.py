#! /usr/bin/python
#
# File:     dhcp_agent_tools.py
# Brief:    Utilities for neutron dhcp agents.
#
# Copyright (c) 2014, Cisco Systems

import logging
import logging.handlers as handlers
import sys
import os
import time
import MySQLdb
from ConfigParser import RawConfigParser
from optparse import OptionParser
from datetime import datetime, timedelta
from urlparse import urlparse
import collections
import random
import subprocess
import pdb

""" ===========================================================
Configurations
=========================================================== """

NUM_ARGS = 0

LOG_FILE = '/var/log/neutron/dhcp_agent_tools.log'
CONF_FILE = '/etc/neutron/neutron.conf'

MY_DB = 'neutron'
MY_TOUT = 15

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 3306
DEFAULT_USER = 'root'
DEFAULT_PASSWD = 'stack'

DEFAULT_AGENT_DOWN_TIME = 75
DEFAULT_DHCP_AGENTS_PER_NETWORK = 2

""" ===========================================================
Neutron Configurable Parameters
=========================================================== """


class MyConfig(object):
    def __init__(self):
        self.parser = RawConfigParser()
        self.parser.optionxform = str
        self.agent_down_time = DEFAULT_AGENT_DOWN_TIME
        self.dhcp_agents_per_network = DEFAULT_DHCP_AGENTS_PER_NETWORK
        self.host = DEFAULT_HOST
        self.port = DEFAULT_PORT
        self.user = DEFAULT_USER
        self.passwd = DEFAULT_PASSWD

    def read(self):
        """
        Read neutron configurable parameters.
        """
        if not os.path.isfile(CONF_FILE):
            logger.warning("%s:%s() %d: missing %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           CONF_FILE)
            raise IOError
        section = 'DEFAULT'
        key1 = 'agent_down_time'
        key2 = 'dhcp_agents_per_network'
        self.parser.read(CONF_FILE)
        try:
            self.agent_down_time = self.parser.getint(section, key1)
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
        try:
            self.dhcp_agents_per_network = self.parser.getint(section, key2)
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
        try:
            url = self.parser.get('database', 'connection')
            parsed = urlparse(url)
            if parsed.hostname is not None:
                self.host = parsed.hostname
            if parsed.port is not None:
                self.port = parsed.port
            if parsed.username is not None:
                self.user = parsed.username
            if parsed.password is not None:
                self.passwd = parsed.password
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
        logger.debug("%s:%s() %d: mysql username %s password %s",
                     self.__class__.__name__,
                     sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                     self.user, self.passwd)
        logger.info("%s:%s() %d: %s is %d, %s is %d", self.__class__.__name__,
                        sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                        key1, self.agent_down_time, key2, self.dhcp_agents_per_network)

""" ===========================================================
Openstaack mySQL database
=========================================================== """


class MyDb(object):
    def __init__(self, host, port, user, passwd, tout):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.tout = tout
        self.conn = None
        self.cur = None
        logger.info("%s:%s() %d: %s %d", self.__class__.__name__,
                    sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                    self.host, self.port)

    def connect(self, db):
        """
        Connect to mysql database.
        input parameters
            db:         database name
        """
        self.db = db
        try:
            logger.debug("%s:%s() %d: connecting to %s", self.__class__.__name__,
                         sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                         self.db)
            self.conn = MySQLdb.connect(host=self.host,
                                        port=self.port,
                                        user=self.user,
                                        passwd=self.passwd,
                                        db=self.db,
                                        connect_timeout=self.tout)
            self.cur = self.conn.cursor()
            logger.debug("%s:%s() %d: connected to %s", self.__class__.__name__,
                         sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                         self.db)
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
            raise

    def disconnect(self):
        """
        Disconnect from mysql database.
        """
        if self.conn is None:
            return
        try:
            self.conn.close()
            self.conn = None
            self.cur = None
            logger.debug("%s:%s() %d: disconnected from %s", self.__class__.__name__,
                         sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                         self.db)
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])

""" ===========================================================
Neutron DHCP Agent to Network Mapping
It is easier to process the data, after reading mysql tables to internal lists,
then to do mysql join.
=========================================================== """


class MyNeutron(object):
    def __init__(self):
        self.config = MyConfig()
        self.config.read()
        self.db = MyDb(self.config.host, self.config.port, self.config.user, self.config.passwd, MY_TOUT)
        try:
            self.db.connect(MY_DB)
        except:
            raise
        self.agents = collections.defaultdict(dict)
        self.networks = {}
        self.agent_in_net = {}
        self.agent_in_net_count = {}
        self.net_in_agent = {}
        self.del_in_agent_count = {}
        self.net_in_ns = {}

    def get_agents(self):
        """
        Query all dhcp agents.
        Populate self.agents
            dictionary key      agent ID
            dictionary value    dictionary of key-value
                                key = host      value = host name
                                key = alive     value = True or False
        """
        if self.db.cur is None:
            return
        start = time.time()
        try:
            s = "SELECT id, host, heartbeat_timestamp FROM agents WHERE topic = 'dhcp_agent' AND admin_state_up = '1'"
            self.db.cur.execute(s)
            rows = self.db.cur.fetchall()
            for row in rows:
                self.net_in_agent[row[0]] = []
                self.net_in_ns[row[0]] = []
                self.agents[row[0]]['host'] = row[1]
                if datetime.utcnow() - row[2] > timedelta(seconds=self.config.agent_down_time):
                    self.agents[row[0]]['alive'] = False
                else:
                    self.agents[row[0]]['alive'] = True
                logger.debug("%s:%s() %d: %s %s", self.__class__.__name__,
                         sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                         row[0], self.agents[row[0]])
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
            raise
        finally:
            duration = time.time() - start
            logger.info("%s:%s() %d: found %d enabled dhcp agents in %.3f seconds",
                        self.__class__.__name__,
                        sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                        len(self.agents), duration)

    def get_networks(self):
        """
        Query all enabled networks.
        Populate self.networks
            dictionary key      network ID
            dictionary value    network name
        """
        if self.db.cur is None:
            return
        start = time.time()
        try:
            s = "SELECT id, name from networks WHERE admin_state_up = '1'"
            self.db.cur.execute(s)
            rows = self.db.cur.fetchall()
            for row in rows:
                self.agent_in_net[row[0]] = []
                self.networks[row[0]] = row[1]
                logger.debug("%s:%s() %d: %s %s", self.__class__.__name__,
                         sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                         row[0], self.networks[row[0]])
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
            raise
        finally:
            duration = time.time() - start
            logger.info("%s:%s() %d: found %d enabled networks in %.3f seconds",
                        self.__class__.__name__,
                        sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                        len(self.networks), duration)

    def get_bindings(self):
        """
        Query all network to dhcp agent bindings.
        Populate self.agent_in_net
            dictionary key      network ID
            dictionary value    list of dhcp agent ID
        Populate self.net_in_agent
            dictionary key      dhcp agent ID
            dictionary value    list of network ID
        """
        if self.db.cur is None:
            return
        count = 0
        start = time.time()
        try:
            s = "SELECT network_id, dhcp_agent_id FROM networkdhcpagentbindings"
            self.db.cur.execute(s)
            rows = self.db.cur.fetchall()
            for row in rows:
                self.agent_in_net[row[0]].append(row[1])
                self.net_in_agent[row[1]].append(row[0])
                logger.debug("%s:%s() %d: %s %s", self.__class__.__name__,
                         sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                         row[0], row[1])
            for uuid in self.agent_in_net:
                num = len(self.agent_in_net[uuid])
                self.agent_in_net_count[str(num)] = self.agent_in_net_count.get(str(num), 0) + 1
                count = count + 1
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
            raise
        finally:
            duration = time.time() - start
            logger.info("%s:%s() %d: found %d bindings for %d networks in %.3f seconds",
                        self.__class__.__name__,
                        sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                        count, len(self.agent_in_net), duration)

    def rm_bindings(self):
        """
        Delete extra dhcp agents in each network.
        """
        if self.db.cur is None:
            return
        count = 0
        start = time.time()
        try:
            for net in self.agent_in_net:
                if len(self.agent_in_net[net]) <= self.config.dhcp_agents_per_network:
                    continue
                alive = []
                dead = []
                del_alive = []
                del_dead = []
                for agent in self.agent_in_net[net]:
                    if self.agents[agent]['alive']:
                        alive.append(agent)
                    else:
                        dead.append(agent)
                n_extra = len(self.agent_in_net[net]) - self.config.dhcp_agents_per_network
                n_extra_dead = min(n_extra, len(dead))
                if n_extra_dead > 0:
                    del_dead = random.sample(dead, n_extra_dead)
                n_extra_alive = n_extra - n_extra_dead
                n_extra_alive = min(n_extra, n_extra_alive)
                if n_extra_alive > 0:
                    del_alive = random.sample(alive, n_extra_alive)
                del_agent = del_dead + del_alive
                for agent in del_agent:
                    s = "DELETE FROM networkdhcpagentbindings WHERE network_id = '%s' AND dhcp_agent_id = '%s'" % (net, agent)
                    self.db.cur.execute(s)
                    self.del_in_agent_count[agent] = self.del_in_agent_count.get(agent, 0) + 1
                    count = count + 1
                    logger.debug("%s:%s() %d: removing %s from DHCP agent in %s", self.__class__.__name__,
                                 sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                                 self.networks[net], self.agents[agent]['host'])
                self.db.conn.commit()
            for agent in self.del_in_agent_count:
                msg = "Removed %d networks for DHCP agent in %s" % (self.del_in_agent_count[agent], self.agents[agent]['host'])
                print("%s" % msg)
                logger.info("%s:%s() %d: %s", self.__class__.__name__,
                            sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                            msg)
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
            raise
        finally:
            duration = time.time() - start
            msg = "Removed %d network-to-agent bindings in %.3f seconds" % (count, duration)
            print("%s" % msg)
            logger.info("%s:%s() %d: %s", self.__class__.__name__,
                        sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                        msg)

    def get_netns(self):
        """
        Get all the network namespace in each network node.
        """
        count = 0
        start = time.time()
        try:
            for agent in self.agents:
                c = "ssh %s -o 'StrictHostKeyChecking=no' 'ip netns | grep qdhcp'" % self.agents[agent]['host']
                pipe = subprocess.Popen(c, shell=True, stdout=subprocess.PIPE).stdout
                for line in pipe:
                    s = line.strip().split('qdhcp-')
                    self.net_in_ns[agent].append(s[1])
                    count = count + 1
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
            raise
        finally:
            duration = time.time() - start
            logger.info("%s:%s() %d: found %d IP network namespace in %.3f seconds",
                        self.__class__.__name__,
                        sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                        count, duration)

    def find_diff(self):
        """
        Find the difference between the set from mySQL and ip network namespace.
        """
        count = 0
        try:
            for agent in self.agents:
                in_agent = [x for x in self.net_in_agent[agent] if x not in self.net_in_ns[agent]]
                in_ns = [x for x in self.net_in_ns[agent] if x not in self.net_in_agent[agent]]
                if len(in_agent) + len(in_ns) > 0:
                    print("DHCP agent in %s:" % self.agents[agent]['host'])
                    for net in in_agent:
                        if net in self.networks:
                            print("  %s %s is in mySQL but not in ip-netns" % (net, self.networks[net]))
                        else:
                            print("  %s is in mySQL but not in net-list" % net)
                        count = count + 1
                    for net in in_ns:
                        if net in self.networks:
                            print("  %s %s is in ip-netns but not in mySQL" % (net, self.networks[net]))
                        else:
                            print("  %s is in ip-netns but not in net-list" % net)
                        count = count + 1
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
            raise
        finally:
            print("Found %d discrepancies in network-to-agent between mySQL and IP network namespace" % count)

    def show_detail(self):
        print("\nFrom MySQL")
        print("==========")
        print("%-40s  %-16s  %s" % ('DHCP Agent UUID', 'Agent Host', 'Alive'))
        print("%-40s  %-16s  %s" % ('---------------', '----------', '-----'))
        for uuid in self.agents:
            print("%-40s  %-16s  %s" % (uuid, self.agents[uuid]['host'], self.agents[uuid]['alive']))

        print("\n%-40s  %s" % ('Network UUID', 'Network Name'))
        print("%-40s  %s" % ('------------', '------------'))
        for uuid in self.networks:
            print("%-40s  %s" % (uuid, self.networks[uuid]))

        print("\n%-40s  %-32s  %s" % ('Network UUID', 'Network Name', 'DHCP agents'))
        print("%-40s  %-32s  %s" % ('------------', '------------', '-----------'))
        for uuid in self.agent_in_net:
            name = self.networks[uuid][:32]
            agents = list(map((lambda x: self.agents[x]['host']), self.agent_in_net[uuid]))
            print("%-40s  %-32s  %s" % (uuid, name, sorted(agents)))

        for uuid in self.agents:
            print("\nDHCP Agent %s in %s hosts %d networks:" % (uuid, self.agents[uuid]['host'], len(self.net_in_agent[uuid])))
            for net in self.net_in_agent[uuid]:
                print("  %s  %s" % (net, self.networks[net]))

        print("\nFrom IP Network Namespace")
        print("=========================")
        for uuid in self.agents:
            print("\nDHCP Agent %s in %s hosts %d networks:" % (uuid, self.agents[uuid]['host'], len(self.net_in_ns[uuid])))
            for net in self.net_in_ns[uuid]:
                print("  %s  %s" % (net, self.networks[net]))

    def show_brief(self):
        print("\nFrom mySQL")
        print("==========")
        print("Total number of networks                 %d" % len(self.networks))
        for n in sorted(self.agent_in_net_count):
            print("  Number of networks with %s DHCP agents  %d" % (n, self.agent_in_net_count[n]))
        for n in sorted(self.net_in_agent):
            print("DHCP agent in %s hosts %d networks" % (self.agents[n]['host'], len(self.net_in_agent[n])))

        print("\nFrom IP Network Namespace")
        print("=========================")
        for n in sorted(self.net_in_agent):
            print("DHCP agent in %s hosts %d networks" % (self.agents[n]['host'], len(self.net_in_ns[n])))

""" ===========================================================
Fast clean up
=========================================================== """


def do_fast_clean():
    try:
        n = MyNeutron()
        n.get_agents()
        n.get_networks()
        n.get_bindings()
        n.rm_bindings()
    except:
        print("\nFailed to complete cleaning up dhcp-agent-list-hosting-net")
        print("Check %s for more details" % LOG_FILE)

""" ===========================================================
Compare mysql with ip network namespace
=========================================================== """


def do_compare():
    try:
        n = MyNeutron()
        n.get_agents()
        n.get_networks()
        n.get_bindings()
        n.get_netns()
        n.find_diff()
    except:
        print("\nFailed to complete comparison of dhcp-agent-list-hosting-net with ip netns")
        print("Check %s for more details" % LOG_FILE)

""" ===========================================================
Display summary
=========================================================== """


def do_brief():
    try:
        n = MyNeutron()
        n.get_agents()
        n.get_networks()
        n.get_bindings()
        n.get_netns()
        n.show_brief()
    except:
        print("\nFailed to show brief information related to dhcp-agent-list-hosting-net")
        print("Check %s for more details" % LOG_FILE)

""" ===========================================================
Display details
=========================================================== """


def do_detail():
    try:
        n = MyNeutron()
        n.get_agents()
        n.get_networks()
        n.get_bindings()
        n.get_netns()
        n.show_detail()
    except:
        print("\nFailed to show detailed information related to dhcp-agent-list-hosting-net")
        print("Check %s for more details" % LOG_FILE)

""" ===========================================================
Tests
=========================================================== """


def do_test():
    print("\nNot implemented")

""" ===========================================================
Main Program.
=========================================================== """


if __name__ == "__main__":
    logger = logging.getLogger('dhcp_agent_tools')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=10485760, backupCount=3)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    handler.setFormatter(formatter)

    brief_help = "Show summary of network-to-agent from mySQL."
    compare_help = "Compare network-to-agent in mySQL and IP network namespace."
    detail_help = "Show details of network-to-agent from mySQL."
    clean_help = ("Fast clean up of extra DHCP agents for each enabled network in mySQL. "
                  "Must restart neutron-dhcp-agent after clean up. "
                  "Recommend to run this before upgrade with all dhcp agents up and running.")
    test_help = "Do not use this option."

    parser = OptionParser()
    parser.add_option('-b', '--brief', action='store_true', dest='brief', help=brief_help, metavar='BRIEF')
    parser.add_option('-c', '--compare', action='store_true', dest='compare', help=compare_help, metavar='COMPARE')
    parser.add_option('-d', '--detail', action='store_true', dest='detail', help=detail_help, metavar='DETAIL')
    parser.add_option('-f', '--fastclean', action='store_true', dest='fast', help=clean_help, metavar='FAST')
    parser.add_option('-t', '--test', action='store_true', dest='test', help=test_help, metavar='TEST')

    flags, args = parser.parse_args()
    if len(args) != NUM_ARGS:
        logger.error("%s() %d: expect %i, get %i arguments!",
                     __name__, sys._getframe().f_lineno, NUM_ARGS, len(args))
        parser.error("expect %i, get %i arguments!" % (NUM_ARGS, len(args)))

    if flags.brief is None and flags.compare is None and \
       flags.detail is None and flags.fast is None and \
       flags.test is None:
        logger.error("%s() %d: expect at least one option!",
                     __name__, sys._getframe().f_lineno)
        parser.error("expect at least one option!")

    if flags.test is True:
        do_test()
    elif flags.brief is True:
        do_brief()
    elif flags.detail is True:
        do_detail()
    elif flags.compare is True:
        do_compare()
    elif flags.fast is True:
        do_fast_clean()
