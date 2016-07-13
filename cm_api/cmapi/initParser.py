#!/usr/bin/env python
import socket
import urllib2
from initServiceAction import *
from optparse import OptionParser
from cm_api.api_client import ApiException
from cm_api.http_client import HttpClient
from cm_api.endpoints.hosts import *
from cm_api.endpoints.services import ApiService

def parse_options():

    cmx_config_options = {'ssh_root_password': None, 'ssh_root_user': 'root', 'ssh_private_key': None,
                          'cluster_name': 'Cluster 1', 'cluster_version': 'CDH5',
                          'username': 'admin', 'password': 'admin', 'cm_server': None,
                          'host_names': None, 'license_file': None,
                          'parcel': [], 'archive_url': 'http://archive.cloudera.com'}

    cmx_config_options.update({'kerberos': {'kdc_host': None, 'security_realm': None,
                                            'kdc_user': None, 'kdc_password': None}})

    def cmx_args(option, opt_str, value, *args, **kwargs):
        if option.dest == 'host_names':
            print "switch %s value check: %s" % (opt_str, value)
            for host in value.split(','):
                if not hostname_resolves(host.strip()):
                    exit(1)
            else:
                cmx_config_options[option.dest] = [socket.gethostbyname(x.strip()) for x in value.split(',')]
        elif option.dest == 'cm_server':
            print "switch %s value check: %s" % (opt_str, value.strip())
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cmx_config_options[option.dest] = socket.gethostbyname(value.strip()) if \
                hostname_resolves(value.strip()) else exit(1)

            if not s.connect_ex((socket.gethostbyname(value), 7180)) == 0:
                print "Cloudera Manager Server is not started on %s " % value
                s.close()
                exit(1)

            # Determine the CM API version
            api_version = get_cm_api_version(cmx_config_options[option.dest],
                                             cmx_config_options['username'],
                                             cmx_config_options['password'])
            print "CM API version: %s" % api_version
            cmx_config_options.update({'api_version': api_version})

            # from CM 5.4+ API v10 we specify 'latest' CDH version with {latest_supported}
            if int(cmx_config_options['api_version'].strip("v")) >= 10:
                cmx_config_options.update({'cdh_version': '5'})
            else:
                cmx_config_options.update({'cdh_version': 'latest'})

        elif option.dest == 'ssh_private_key':
            with open(value, 'r') as f:
                cmx_config_options[option.dest] = f.read()
        elif option.dest == 'cdh_version':
            print "switch %s value check: %s" % (opt_str, value)
            _cdh_repo = urllib2.urlopen("%s/cdh5/parcels/" % cmx_config_options["archive_url"]).read()
            _cdh_ver = [link.replace('/', '') for link in re.findall(r"<a.*?\s*href=\".*?\".*?>(.*?)</a>", _cdh_repo)
                        if link not in ['Name', 'Last modified', 'Size', 'Description', 'Parent Directory']]
            cmx_config_options[option.dest] = value
            if value not in _cdh_ver:
                print "Invalid CDH version: %s" % value
                exit(1)
        else:
            cmx_config_options[option.dest] = value

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

    parser = OptionParser()
    parser.add_option('-d', '--teardown', dest='teardown', action="store", type="string",
                      help='Teardown Cloudera Manager Cluster. Required arguments "keep_cluster" or "remove_cluster".')
    parser.add_option('-i', '--cdh-version', dest='cdh_version', type="string", action='callback',
                      callback=cmx_args, default='latest', help='Install CDH version. Default "latest"')
    parser.add_option('-k', '--ssh-private-key', dest='ssh_private_key', type="string", action='callback',
                      callback=cmx_args, help='The private key to authenticate with the hosts. '
                                              'Specify either this or a password.')
    parser.add_option('-l', '--license-file', dest='license_file', type="string", action='callback',
                      callback=cmx_args, help='Cloudera Manager License file name')
    parser.add_option('-m', '--cm-server', dest='cm_server', type="string", action='callback', callback=cmx_args,
                      help='*Set Cloudera Manager Server Host. '
                           'Note: This is the host where the Cloudera Management Services get installed.')
    parser.add_option('-n', '--cluster-name', dest='cluster_name', type="string", action='callback',
                      callback=cmx_args, default='Cluster 1',
                      help='Set Cloudera Manager Cluster name enclosed in double quotes. Default "Cluster 1"')
    parser.add_option('-p', '--ssh-root-password', dest='ssh_root_password', type="string", action='callback',
                      callback=cmx_args, help='*Set target node(s) ssh password..')
    parser.add_option('-u', '--ssh-root-user', dest='ssh_root_user', type="string", action='callback',
                      callback=cmx_args, default='root', help='Set target node(s) ssh username. Default root')
    parser.add_option('-w', '--host-names', dest='host_names', type="string", action='callback',
                      callback=cmx_args,
                      help='*Set target node(s) list, separate with comma eg: -w host1,host2,...,host(n). '
                           'Note:'
                           ' - enclose in double quote.'
                           ' - CM_SERVER excluded in this list, if you want install CDH Services in CM_SERVER'
                           ' add the host to this list.')

    (options, args) = parser.parse_args()

    msg_req_args = "Please specify the required arguments: "
    if cmx_config_options['cm_server'] is None:
        parser.error(msg_req_args + "-m/--cm-server")
    else:
        if not (cmx_config_options['ssh_private_key'] or cmx_config_options['ssh_root_password']):
            parser.error(msg_req_args + "-p/--ssh-root-password or -k/--ssh-private-key")
        elif cmx_config_options['host_names'] is None:
            parser.error(msg_req_args + "-w/--host-names")
        elif cmx_config_options['ssh_private_key'] and cmx_config_options['ssh_root_password']:
            parser.error(msg_req_args + "-p/--ssh-root-password _OR_ -k/--ssh-private-key")

    # Management services password. They are required when adding Management services
    manager = ManagementActions
    if not (bool(manager.get_mgmt_password("ACTIVITYMONITOR"))
            and bool(manager.get_mgmt_password("REPORTSMANAGER"))):
        cmx_config_options['amon_password'] = bool(manager.get_mgmt_password("ACTIVITYMONITOR"))
        cmx_config_options['rman_password'] = bool(manager.get_mgmt_password("REPORTSMANAGER"))
    else:
        cmx_config_options['amon_password'] = manager.get_mgmt_password("ACTIVITYMONITOR")
        cmx_config_options['rman_password'] = manager.get_mgmt_password("REPORTSMANAGER")

    cmx = type('', (), cmx_config_options)
    check = ActiveCommands()
    cdh = ServiceActions
    if cmx_config_options['cm_server'] and options.teardown:
        if options.teardown.lower() in ['remove_cluster', 'keep_cluster']:
            teardown(keep_cluster=(options.teardown.lower() == 'keep_cluster'))
            print "Bye!"
            exit(0)
        else:
            print 'Teardown Cloudera Manager Cluster. Required arguments "keep_cluster" or "remove_cluster".'
            exit(1)

    # Uncomment here to see cmx configuration options
    # print cmx_config_options
    return options
