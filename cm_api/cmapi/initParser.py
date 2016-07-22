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

#    SSH_ROOT_PASS       = CONFIG.get("SSH", "ssh.password")
#    SSH_ROOT_USER       = CONFIG.get("SSH", "ssh.user")
#    SSH_PRIVATE_KEY     = CONFIG.get("SSH", "ssh.privateKey")

#    CLUSTER_CM_SERVER   = CONFIG.get("CLUSTER", "cluster.cm.server")
#    CLUSTER_HOSTS       = CONFIG.get("CLUSTER", "cluster.hosts")
#    CLUSTER_NAME        = CONFIG.get("CLUSTER", "cluster.name")
#    CLUSTER_VERSION     = CONFIG.get("CLUSTER", "cluster.version")
#    CLUSTER_SERVICES    = CONFIG.get("CLUSTER", "cluster.services")

#    LICENSE_FILE        = CONFIG.get("LICENSE", "license.file.cm")

#    config_options  = {'ssh_root_password': None,
#                           'ssh_root_user': 'root',
#                           'ssh_private_key': None,
#                           'cluster_name': 'Cluster 1',
#                           'cluster_version': 'CDH5',
#                           'username': 'admin',
#                           'password': 'admin',
#                           'cm_server': None,
#                           'host_names': None,
#                           'license_file': None,
#                           'archive_url': 'http://archive.cloudera.com',
#                           'parcel': []
#                           }

    config_options  = {'username': 'admin','password': 'admin','parcel': []}
    config_options.update({'ssh_root_password': CONFIG.get("SSH", "ssh.password")})
    config_options.update({'ssh_root_user': CONFIG.get("SSH", "ssh.user")})
    with open(CONFIG.get("SSH", "ssh.privateKey"), 'r') as f:
        config_options['ssh_private_key'] = f.read()

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

    config_options.update({'cm_services': [CONFIG.get("CLUSTER", "cluster.services").split(',')]})
    print "Services to be added ".join(config_options['cm_services'])

    api_version = get_cm_api_version(config_options['cm_server'],config_options['username'],config_options['password'])
    print "CM API version: %s" % api_version
    config_options.update({'api_version': api_version})

    config_options.update({'cdh_version': 'latest'})

    config_options.update({'license_file': CONFIG.get("LICENSE", "license.file.cm")})

    config_options.update({'kerberos': {'kdc_host': None, 'security_realm': None,'kdc_user': None, 'kdc_password': None}})

#    def cmx_args(option, opt_str, value, *args, **kwargs):
#        if option.dest == 'host_names':
#            print "switch %s value check: %s" % (opt_str, value)
#            for host in value.split(','):
#                if not hostname_resolves(host.strip()):
#                    exit(1)
#            else:
#                config_options[option.dest] = [socket.gethostbyname(x.strip()) for x in value.split(',')]
#        elif option.dest == 'cm_server':
#            print "switch %s value check: %s" % (opt_str, value.strip())
#            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#            config_options[option.dest] = socket.gethostbyname(value.strip()) if \
#                hostname_resolves(value.strip()) else exit(1)
#
#            if not s.connect_ex((socket.gethostbyname(value), 7180)) == 0:
#                print "Cloudera Manager Server is not started on %s " % value
#                s.close()
#                exit(1)

            # Determine the CM API version
#            api_version = get_cm_api_version(config_options[option.dest],
#                                             config_options['username'],
#                                             config_options['password'])
#            print "CM API version: %s" % api_version
#            config_options.update({'api_version': api_version})

            # from CM 5.4+ API v10 we specify 'latest' CDH version with {latest_supported}
#            if int(config_options['api_version'].strip("v")) >= 10:
#                config_options.update({'cdh_version': '5'})
#            else:
#                config_options.update({'cdh_version': 'latest'})

#        elif option.dest == 'ssh_private_key':
#            with open(value, 'r') as f:
#                config_options[option.dest] = f.read()
#        elif option.dest == 'cdh_version':
#            print "switch %s value check: %s" % (opt_str, value)
#            _cdh_repo = urllib2.urlopen("%s/cdh5/parcels/" % config_options["archive_url"]).read()
#            _cdh_repo = urllib2.urlopen("http://archive.cloudera.com/cdh5/parcels/").read()
#            _cdh_ver = [link.replace('/', '') for link in re.findall(r"<a.*?\s*href=\".*?\".*?>(.*?)</a>", _cdh_repo)
#                        if link not in ['Name', 'Last modified', 'Size', 'Description', 'Parent Directory']]
#            config_options[option.dest] = value
#            if value not in _cdh_ver:
#                print "Invalid CDH version: %s" % value
#                exit(1)
#        else:
#            config_options[option.dest] = value



#    parser = OptionParser()
#    parser.add_option('-d', '--teardown', dest='teardown', action="store", type="string",
#                      help='Teardown Cloudera Manager Cluster. Required arguments "keep_cluster" or "remove_cluster".')
#    parser.add_option('-i', '--cdh-version', dest='cdh_version', type="string", action='callback',
#                      callback=cmx_args, default='latest', help='Install CDH version. Default "latest"')
#    parser.add_option('-k', '--ssh-private-key', dest='ssh_private_key', type="string", action='callback',
#                      callback=cmx_args, help='The private key to authenticate with the hosts. '
#                                              'Specify either this or a password.')
#    parser.add_option('-l', '--license-file', dest='license_file', type="string", action='callback',
#                      callback=cmx_args, help='Cloudera Manager License file name')
#    parser.add_option('-m', '--cm-server', dest='cm_server', type="string", action='callback', callback=cmx_args,
#                      help='*Set Cloudera Manager Server Host. '
#                           'Note: This is the host where the Cloudera Management Services get installed.')
#    parser.add_option('-n', '--cluster-name', dest='cluster_name', type="string", action='callback',
#                      callback=cmx_args, default='Cluster 1',
#                      help='Set Cloudera Manager Cluster name enclosed in double quotes. Default "Cluster 1"')
#    parser.add_option('-p', '--ssh-root-password', dest='ssh_root_password', type="string", action='callback',
#                      callback=cmx_args, help='*Set target node(s) ssh password..')
#    parser.add_option('-u', '--ssh-root-user', dest='ssh_root_user', type="string", action='callback',
#                      callback=cmx_args, default='root', help='Set target node(s) ssh username. Default root')
#    parser.add_option('-w', '--host-names', dest='host_names', type="string", action='callback',
#                      callback=cmx_args,
#                      help='*Set target node(s) list, separate with comma eg: -w host1,host2,...,host(n). '
#                           'Note:'
#                           ' - enclose in double quote.'
#                           ' - CM_SERVER excluded in this list, if you want install CDH Services in CM_SERVER'
#                           ' add the host to this list.')
#
#    (options, args) = parser.parse_args()

#    msg_req_args = "Please specify the required arguments: "
#    if config_options['cm_server'] is None:
#        parser.error(msg_req_args + "-m/--cm-server")
#    else:
#        if not (config_options['ssh_private_key'] or config_options['ssh_root_password']):
#            parser.error(msg_req_args + "-p/--ssh-root-password or -k/--ssh-private-key")
#        elif config_options['host_names'] is None:
#            parser.error(msg_req_args + "-w/--host-names")
#        elif config_options['ssh_private_key'] and config_options['ssh_root_password']:
#            parser.error(msg_req_args + "-p/--ssh-root-password _OR_ -k/--ssh-private-key")

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
#    if config_options['cm_server'] and options.teardown:
#        if options.teardown.lower() in ['remove_cluster', 'keep_cluster']:
#            teardown(keep_cluster=(options.teardown.lower() == 'keep_cluster'))
#            print "Bye!"
#            exit(0)
#        else:
#            print 'Teardown Cloudera Manager Cluster. Required arguments "keep_cluster" or "remove_cluster".'
#            exit(1)

    # Uncomment here to see cmx configuration options
    # print config_options
    return options
