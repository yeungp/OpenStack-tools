#! /usr/bin/python
#
# File:     neutron_tools.py
# Brief:    Utilities for neutron.
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

LOG_DIR = '/var/log/neutron/'
LOG_FILE = LOG_DIR + 'neutron_tools.log'
NEUTRON_CONF = '/etc/neutron/neutron.conf'

NEUTRON_DB = 'neutron'
MY_TOUT = 15

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 3306
DEFAULT_USER = 'root'
DEFAULT_PASSWD = 'stack'

DEFAULT_AGENT_DOWN_TIME = 75
DEFAULT_DHCP_AGENTS_PER_NETWORK = 2

KEYSTONE_CONF = '/etc/keystone/keystone.conf'
KEYSTONE_DB = 'keystone'

""" ===========================================================
Configurable Parameters
Initially designed for neutron, but extended for other services.
=========================================================== """


class MyConfig(object):
    def __init__(self, conf_file, section):
        """
        conf_file   service configuration file
        section     section name where mysql connection is defined
        """
        self.conf_file = conf_file
        self.section = section
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
        Read configurable parameters.
        """
        if not os.path.isfile(self.conf_file):
            logger.warning("%s:%s() %d: missing %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           self.conf_file)
            raise IOError
        self.parser.read(self.conf_file)
        if self.conf_file == NEUTRON_CONF:
            self.read_neutron_param()
        self.read_mysql_param()

    def read_neutron_param(self):
        """
        Read specific neutron parameters.
        """
        section = 'DEFAULT'
        key1 = 'agent_down_time'
        key2 = 'dhcp_agents_per_network'
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
        logger.info("%s:%s() %d: read %s, use %s = %d, %s = %d", self.__class__.__name__,
                    sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                    os.path.basename(self.conf_file),
                    key1, self.agent_down_time, key2, self.dhcp_agents_per_network)

    def read_mysql_param(self):
        """
        Read specific mysql parameters.
        """
        try:
            url = self.parser.get(self.section, 'connection')
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
        logger.debug("%s:%s() %d: read %s, use username = %s, password = %s",
                     self.__class__.__name__,
                     sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                     os.path.basename(self.conf_file), self.user, self.passwd)

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
        logger.info("%s:%s() %d: %s:%d, user is %s", self.__class__.__name__,
                    sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                    self.host, self.port, self.user)

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
        self.config = MyConfig(NEUTRON_CONF, 'database')
        self.config.read()
        self.db = MyDb(self.config.host, self.config.port,
                       self.config.user, self.config.passwd, MY_TOUT)
        try:
            self.db.connect(NEUTRON_DB)
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
                    logger.debug("%s:%s() %d: removing %s from DHCP agent in %s",
                                 self.__class__.__name__,
                                 sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                                 self.networks[net], self.agents[agent]['host'])
                self.db.conn.commit()
            for agent in self.del_in_agent_count:
                msg = "Removed %d networks for DHCP agent in %s" % (
                      self.del_in_agent_count[agent], self.agents[agent]['host'])
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
                c = "ssh %s -o 'StrictHostKeyChecking=no' 'ip netns | grep qdhcp'" % (
                    self.agents[agent]['host'])
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
                            print("  %s %s is in mySQL but not in ip-netns" %
                                 (net, self.networks[net]))
                        else:
                            print("  %s is in mySQL but not in net-list" % net)
                        count = count + 1
                    for net in in_ns:
                        if net in self.networks:
                            print("  %s %s is in ip-netns but not in mySQL" %
                                 (net, self.networks[net]))
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
            print("%-40s  %-16s  %s" %
                 (uuid, self.agents[uuid]['host'], self.agents[uuid]['alive']))

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
            print("\nDHCP Agent %s in %s hosts %d networks:" %
                 (uuid, self.agents[uuid]['host'], len(self.net_in_agent[uuid])))
            for net in self.net_in_agent[uuid]:
                print("  %s  %s" % (net, self.networks[net]))

        print("\nFrom IP Network Namespace")
        print("=========================")
        for uuid in self.agents:
            print("\nDHCP Agent %s in %s hosts %d networks:" %
                 (uuid, self.agents[uuid]['host'], len(self.net_in_ns[uuid])))
            for net in self.net_in_ns[uuid]:
                print("  %s  %s" % (net, self.networks[net]))

    def show_brief(self):
        print("\nFrom mySQL")
        print("==========")
        print("Total number of networks                 %d" % len(self.networks))
        for n in sorted(self.agent_in_net_count):
            print("  Number of networks with %s DHCP agents  %d" %
                 (n, self.agent_in_net_count[n]))
        for n in sorted(self.net_in_agent):
            print("DHCP agent in %s hosts %d networks" %
                 (self.agents[n]['host'], len(self.net_in_agent[n])))

        print("\nFrom IP Network Namespace")
        print("=========================")
        for n in sorted(self.net_in_agent):
            print("DHCP agent in %s hosts %d networks" %
                 (self.agents[n]['host'], len(self.net_in_ns[n])))

""" ===========================================================
DHCP Agent
=========================================================== """


class DhcpAgent(object):
    def __init__(self):
        try:
            self.n = MyNeutron()
        except:
            print("\nFailed to access neutron database")
            print("Check %s for more details" % LOG_FILE)

    def fast_clean(self):
        """
        Fast clean up
        """
        try:
            self.n.get_agents()
            self.n.get_networks()
            self.n.get_bindings()
            self.n.rm_bindings()
        except:
            print("\nFailed to complete cleaning up dhcp-agent-list-hosting-net")
            print("Check %s for more details" % LOG_FILE)

    def compare(self):
        """
        Compare mysql with ip network namespace
        """
        try:
            self.n.get_agents()
            self.n.get_networks()
            self.n.get_bindings()
            self.n.get_netns()
            self.n.find_diff()
        except:
            print("\nFailed to complete comparison of dhcp-agent-list-hosting-net with ip netns")
            print("Check %s for more details" % LOG_FILE)

    def show_brief(self):
        """
        Display summary
        """
        try:
            self.n.get_agents()
            self.n.get_networks()
            self.n.get_bindings()
            self.n.get_netns()
            self.n.show_brief()
        except:
            print("\nFailed to show brief information related to dhcp-agent-list-hosting-net")
            print("Check %s for more details" % LOG_FILE)

    def show_detail(self):
        """
        Display details
        """
        try:
            self.n.get_agents()
            self.n.get_networks()
            self.n.get_bindings()
            self.n.get_netns()
            self.n.show_detail()
        except:
            print("\nFailed to show detailed information related to dhcp-agent-list-hosting-net")
            print("Check %s for more details" % LOG_FILE)

    def help(self):
        """
        Display help message
        """
        print("neutron_tools.py [ -b | -c | -d | -f ] dhcp-agent\n")
        print("This utility affects 'neutron dhcp-agent-list-hosting-net'.")
        print("  -b --brief            Show summary of networks hosted by DHCP agents from mySQL.")
        print("  -c --compare          Compare network hosted by DHCP agents from mySQL and IP network namespace.")
        print("  -d --detail           Show details of networks hosted by DHCP agents from mySQL.")
        print("  -f --fastclean        Fast clean up of extra DHCP agents for each enabled network in mySQL.\n"
            "\t\t\tMust restart neutron-dhcp-agent after clean up.\n"
            "\t\t\tRecommend to run this before upgrade with all dhcp agents up and running.")

""" ===========================================================
Security Groups in Tenants
It is easier to process the data, after reading mysql tables to internal lists,
then to do mysql join.
=========================================================== """


class MyTenantSecurityGroup(object):
    def __init__(self):
        config = MyConfig(NEUTRON_CONF, 'database')
        config.read()
        self.db = MyDb(config.host, config.port, config.user, config.passwd, MY_TOUT)
        try:
            self.db.connect(NEUTRON_DB)
        except:
            raise
        self.tenants = collections.defaultdict(dict)
        self.orphans = collections.defaultdict(dict)
        self.n_groups = 0
        self.n_groups_in_tenants = 0
        self.n_groups_in_orphans = 0

    def get_tenants(self):
        """
        Query all tenants, similar to "keystone tenant-list".
        Populate self.tenants
            dictionary key      tenant ID
            dictionary value    dictionary of key-value
                                key = name      value = tenant name
                                key = group     value = list of group name
        """
        keystone = MyConfig(KEYSTONE_CONF, 'sql')
        keystone.read()
        kdb = MyDb(keystone.host, keystone.port, keystone.user, keystone.passwd, MY_TOUT)
        try:
            kdb.connect(KEYSTONE_DB)
        except:
            raise
        if kdb.cur is None:
            return
        start = time.time()
        try:
            s = "SELECT id, name FROM project"
            kdb.cur.execute(s)
            rows = kdb.cur.fetchall()
            for row in rows:
                self.tenants[row[0]]['name'] = row[1]
                self.tenants[row[0]]['group'] = []
                logger.debug("%s:%s() %d: %s %s", self.__class__.__name__,
                             sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                             row[0], self.tenants[row[0]])
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
            raise
        finally:
            kdb.disconnect()
            duration = time.time() - start
            logger.info("%s:%s() %d: found %d tenants in %.3f seconds",
                        self.__class__.__name__,
                        sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                        len(self.tenants), duration)

    def get_secgroups(self):
        """
        Query all security groups, similar to "neutron security-group-list"
        Populate self.tenants
            dictionary key      tenant ID
            dictionary value    dictionary of key-value
                                key = name      value = tenant name
                                key = group     value = list of group name
        Populate self.orphans
            dictionary key      tenant ID
            dictionary value    list of group name
        """
        if self.db.cur is None:
            return
        start = time.time()
        self.n_groups = 0
        self.n_groups_in_tenants = 0
        self.n_groups_in_orphans = 0
        try:
            s = "SELECT tenant_id, name FROM securitygroups"
            self.db.cur.execute(s)
            rows = self.db.cur.fetchall()
            for row in rows:
                if row[0] in self.tenants:
                    self.tenants[row[0]]['group'].append(row[1])
                    self.n_groups_in_tenants = self.n_groups_in_tenants + 1
                    logger.debug("%s:%s() %d: %s %s", self.__class__.__name__,
                                 sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                                 row[0], self.tenants[row[0]])
                else:
                    if row[0] not in self.orphans:
                        self.orphans[row[0]] = []
                    self.orphans[row[0]].append(row[1])
                    self.n_groups_in_orphans = self.n_groups_in_orphans + 1
                    logger.debug("%s:%s() %d: %s %s", self.__class__.__name__,
                                 sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                                 row[0], self.orphans[row[0]])
                self.n_groups = self.n_groups + 1
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
            raise
        finally:
            duration = time.time() - start
            logger.info("%s:%s() %d: found %d security groups in %.3f seconds",
                        self.__class__.__name__,
                        sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                        self.n_groups, duration)

    def rm_secgroups(self):
        """
        Chunk delete orphaned security groups
        """
        if self.db.cur is None:
            return
        count = 0
        start = time.time()
        try:
            for tenant in self.orphans:
                s = "DELETE FROM securitygroups WHERE tenant_id = '%s'" % (tenant)
                self.db.cur.execute(s)
                self.db.conn.commit()
                count = count + len(self.orphans[tenant])
                logger.debug("%s:%s() %d: removed %d security groups from tenant %s",
                             self.__class__.__name__,
                             sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                             len(self.orphans[tenant]), tenant)
        except:
            logger.warning("%s:%s() %d: %s %s", self.__class__.__name__,
                           sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                           sys.exc_info()[0], sys.exc_info()[1])
            raise
        finally:
            duration = time.time() - start
            logger.info("%s:%s() %d: removed %d security groups in %.3f seconds",
                        self.__class__.__name__,
                        sys._getframe().f_code.co_name, sys._getframe().f_lineno,
                        count, duration)

    def show_brief(self):
        print("Total number of security groups                   %d" % self.n_groups)
        print("Number of security groups in %03d active tenants   %d" %
             (len(self.tenants), self.n_groups_in_tenants))
        print("Number of security groups in %03d deleted tenants  %d" %
             (len(self.orphans), self.n_groups_in_orphans))

    def show_detail(self):
        print("\n%-40s  %s" % ('Active Tenant', 'Security Groups'))
        print("%-40s  %s" % ('-------------', '---------------'))
        items = {}
        for tenant in self.tenants:
            items[self.tenants[tenant]['name']] = ', '.join(self.tenants[tenant]['group'])
        for key in sorted(items):
            print("%-40s  %s" % (key, items[key]))
        print("\n%-40s  %s" % ('Deleted Tenant', 'Security Groups'))
        print("%-40s  %s" % ('--------------', '---------------'))
        for tenant in self.orphans:
            print("%-40s  %s" % (tenant, ', '.join(self.orphans[tenant])))

""" ===========================================================
Security Group
=========================================================== """


class SecurityGroup(object):
    def __init__(self):
        try:
            self.t = MyTenantSecurityGroup()
        except:
            print("\nFailed to access neutron database")
            print("Check %s for more details" % LOG_FILE)

    def fast_clean(self):
        """
        Fast clean up
        """
        try:
            self.t.get_tenants()
            self.t.get_secgroups()
            self.t.rm_secgroups()
        except:
            print("\nFailed to complete cleaning up security-group-list")
            print("Check %s for more details" % LOG_FILE)

    def show_brief(self):
        """
        Display summary
        """
        try:
            self.t.get_tenants()
            self.t.get_secgroups()
            self.t.show_brief()
        except:
            print("\nFailed to show brief information related to tenant-list and security-group-list")
            print("Check %s for more details" % LOG_FILE)

    def show_detail(self):
        """
        Display detailed information
        """
        try:
            self.t.get_tenants()
            self.t.get_secgroups()
            self.t.show_detail()
        except:
            print("\nFailed to show detailed information related to tenant-list and security-group-list")
            print("Check %s for more details" % LOG_FILE)

    def help(self):
        """
        Display help message
        """
        print("neutron_tools.py [ -b | -d | -f ] security-group\n")
        print("This utility affects 'neutron security-group-list'.")
        print("  -b --brief            Show summary of security groups in tenants from mySQL.")
        print("  -d --detail           Show details of security groups in tenants from mySQL.")
        print("  -f --fastclean        Fast clean up of security groups not belonged to any tenant in mySQL.\n")

""" ===========================================================
Tests
=========================================================== """


def do_test():
    print("\nNot implemented")

""" ===========================================================
Logging
=========================================================== """
logger = logging.getLogger('neutron_tools')


def do_logging():
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=10485760, backupCount=3)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    handler.setFormatter(formatter)

""" ===========================================================
Option Parsing
=========================================================== """
NUM_ARGS = 1


def do_parsing():
    brief_help = "Show information summary."
    compare_help = "Compare information in different stores."
    detail_help = "Show detailed information."
    fastclean_help = "Fast clean up directly in mySQL."
    help_help = "Show this help message and exit."
    test_help = "Do not use this option."

    parser = OptionParser(add_help_option=False)
    parser.add_option('-b', '--brief', action='store_true', dest='brief', help=brief_help, metavar='BRIEF')
    parser.add_option('-c', '--compare', action='store_true', dest='compare', help=compare_help, metavar='COMPARE')
    parser.add_option('-d', '--detail', action='store_true', dest='detail', help=detail_help, metavar='DETAIL')
    parser.add_option('-f', '--fastclean', action='store_true', dest='fast', help=fastclean_help, metavar='FAST')
    parser.add_option('-h', '--help', action='store_true', dest='help', help=help_help, metavar='HELP')
    parser.add_option('-t', '--test', action='store_true', dest='test', help=test_help, metavar='TEST')

    def usage():
        parser.print_help()
        print "\nSupported Operations:"
        print "  neutron_tools.py [ -b | -c | -d | -f ] dhcp-agent"
        print "  neutron_tools.py [ -b | -d | -f ] security-group"
        print "More Helps:"
        print "  neutron_tools.py -h dhcp-agent"
        print "  neutron_tools.py -h security-group"
        sys.exit()

    flags, args = parser.parse_args()

    if flags.help and len(args) == 0:
        usage()

    if flags.test:
        do_test()
        sys.exit()

    if len(args) != NUM_ARGS:
        logger.error("%s() %d: expect %i, get %i arguments!",
                     __name__, sys._getframe().f_lineno, NUM_ARGS, len(args))
        print("\nExpect %i, get %i arguments!\n" % (NUM_ARGS, len(args)))
        usage()

    if (args[0] != 'dhcp-agent' and
        args[0] != 'security-group'):
        logger.error("%s() %d: unsupported argument",
                     __name__, sys._getframe().f_lineno)
        print("\nUnsupported argument!\n")
        usage()

    if args[0] == 'dhcp-agent':
        agent = DhcpAgent()
        if (flags.brief is None and flags.compare is None and
            flags.detail is None and flags.fast is None and
            flags.help is None):
            logger.error("%s() %d: missing or invalid options",
                         __name__, sys._getframe().f_lineno)
            print("\nMissing or invalid options!\n")
            usage()
        if flags.help:
            agent.help()
        elif flags.brief is True:
            agent.show_brief()
        elif flags.detail is True:
            agent.show_detail()
        elif flags.compare is True:
            agent.compare()
        elif flags.fast is True:
            agent.fast_clean()
    elif args[0] == 'security-group':
        sec = SecurityGroup()
        if (flags.brief is None and flags.detail is None and
            flags.fast is None and flags.help is None):
            logger.error("%s() %d: missing or invalid options",
                         __name__, sys._getframe().f_lineno)
            print("\nMissing or invalid options!\n")
            usage()
        if flags.help:
            sec.help()
        elif flags.brief is True:
            sec.show_brief()
        elif flags.detail is True:
            sec.show_detail()
        elif flags.fast is True:
            sec.fast_clean()

""" ===========================================================
Main Program.
=========================================================== """

if __name__ == "__main__":
    do_logging()
    do_parsing()
