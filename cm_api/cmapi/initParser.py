#!/usr/bin/env python
import socket
import urllib2
import ConfigParser
from initServiceAction import *
from hadoopServices import initVar
from optparse import OptionParser
from cm_api.api_client import ApiException
from cm_api.http_client import HttpClient
from cm_api.endpoints.hosts import *
from cm_api.endpoints.services import ApiService


def parse_options():
    def hostname_resolves(hostname):
        """
        Check if hostname resolves
        :param hostname:
        :return:
        """
        try:
            if socket.gethostbyname(hostname) == '0.0.0.0':
                print "Error [{'host': '%s', 'fqdn': '%s'}]" % \
                      (socket.gethostbyname(hostname), socket.getfqdn(hostname))
                return False
            else:
                print "Success [{'host': '%s', 'fqdn': '%s'}]" % \
                      (socket.gethostbyname(hostname), socket.getfqdn(hostname))
                return True
        except socket.error:
            print "Error 'host': '%s'" % hostname
            return False

    def get_cm_api_version(cm_server, username, password):
        """
        Get supported API version from CM
        :param cm_server:
        :param username:
        :param password:
        :return version:
        """
        base_url = "%s://%s:%s/api" % ("http", cm_server, 7180)
        client = HttpClient(base_url, exc_class=ApiException)
        client.set_basic_auth(username, password, "Cloudera Manager")
        client.set_headers({"Content-Type": "application/json"})
        return client.execute("GET", "/version").read().strip('v')

    CONFIG = ConfigParser.ConfigParser()
    CONFIG.read("cm_config.ini")

    config_options  = {'username': 'admin','password': 'admin','parcel': []}
    config_options.update({'ssh_root_password': CONFIG.get("SSH", "ssh.password")})
    config_options.update({'ssh_private_key': CONFIG.get("SSH", "ssh.privateKey")})
    config_options.update({'ssh_root_user': CONFIG.get("SSH", "ssh.user")})
    with open(CONFIG.get("SSH", "ssh.privateKey"), 'r') as f:
        config_options['ssh_private_key'] = f.read()
    print config_options['ssh_private_key']
    config_options.update({'cluster_name': CONFIG.get("CLUSTER", "cluster.name")})
    CLUSTER_HOSTS       = CONFIG.get("CLUSTER", "cluster.hosts")
    for host in CLUSTER_HOSTS.split(','):
        print "Adding Hosts %s " % host
        if not hostname_resolves(host.strip()):
            exit(1)
        else:
            config_options['host_names'] = [socket.gethostbyname(x.strip()) for x in CLUSTER_HOSTS.split(',')]

    CLUSTER_CM_SERVER   = CONFIG.get("CLUSTER", "cluster.cm.server")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    config_options['cm_server'] = socket.gethostbyname(CLUSTER_CM_SERVER.strip()) if \
        hostname_resolves(CLUSTER_CM_SERVER.strip()) else exit(1)

    if not s.connect_ex((socket.gethostbyname(CLUSTER_CM_SERVER), 7180)) == 0:
        print "Cloudera Manager Server is not started on %s " % CLUSTER_CM_SERVER
        s.close()
        exit(1)

    #config_options.update({'cm_services': [CONFIG.get("CLUSTER", "cluster.services").split(',')]})
    #print "Services to be added ".join(config_options['cm_services'])
    config_options.update({'cluster_version': 'CDH5'})
    api_version = get_cm_api_version(config_options['cm_server'],config_options['username'],config_options['password'])
    print "CM API version: %s" % api_version
    config_options.update({'api_version': api_version})

    config_options.update({'cdh_version': 'latest'})

    config_options.update({'license_file': CONFIG.get("LICENSE", "license.file.cm")})

    config_options.update({'kerberos': {'kdc_host': None, 'security_realm': None,'kdc_user': None, 'kdc_password': None}})

    # Management services password. They are required when adding Management services
    initVar.manager = ManagementActions
    if not (bool(initVar.manager.get_mgmt_password("ACTIVITYMONITOR"))
            and bool(initVar.manager.get_mgmt_password("REPORTSMANAGER"))):
        config_options['amon_password'] = bool(initVar.manager.get_mgmt_password("ACTIVITYMONITOR"))
        config_options['rman_password'] = bool(initVar.manager.get_mgmt_password("REPORTSMANAGER"))
    else:
        config_options['amon_password'] = initVar.manager.get_mgmt_password("ACTIVITYMONITOR")
        config_options['rman_password'] = initVar.manager.get_mgmt_password("REPORTSMANAGER")

    initVar.cmx = type('', (), config_options)
    initVar.check = ActiveCommands()
    initVar.cdh = ServiceActions

    return True
