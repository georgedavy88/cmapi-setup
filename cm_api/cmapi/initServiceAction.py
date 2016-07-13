#!/usr/bin/env python
import socket
import hashlib
import os
import sys
from initVar import *
from cm_api.api_client import ApiResource, ApiException
from cm_api.endpoints.hosts import *
from cm_api.endpoints.services import ApiServiceSetupInfo, ApiService

class ManagementActions:
    """
    Example stopping 'ACTIVITYMONITOR', 'REPORTSMANAGER' Management Role
    :param role_list:
    :param action:
    :return:
    """

    def __init__(self, *role_list):
        self._role_list = role_list
        self._api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password,
                                version=initVar.cmx.api_version)
        self._cm = self._api.get_cloudera_manager()
        try:
            self._service = self._cm.get_service()
        except ApiException:
            self._service = self._cm.create_mgmt_service(ApiServiceSetupInfo())
        self._role_types = [x.type for x in self._service.get_all_roles()]

    def stop(self):
        self._role_action('stop_roles')

    def start(self):
        self._role_action('start_roles')

    def restart(self):
        self._role_action('restart_roles')

    def _role_action(self, action):
        state = {'start_roles': ['STOPPED'], 'stop_roles': ['STARTED'], 'restart_roles': ['STARTED', 'STOPPED']}
        for mgmt_role in [x for x in self._role_list if x in self._role_types]:
            for role in [x for x in self._service.get_roles_by_type(mgmt_role) if x.roleState in state[action]]:
                [check.status_for_command("%s role %s" % (action.split("_")[0].upper(), mgmt_role), cmd)
                 for cmd in getattr(self._service, action)(role.name)]

    def setup(self):
        """
        Setup Management Roles
        'ACTIVITYMONITOR', 'ALERTPUBLISHER', 'EVENTSERVER', 'HOSTMONITOR', 'SERVICEMONITOR'
        Requires License: 'NAVIGATOR', 'NAVIGATORMETASERVER', 'REPORTSMANAGER"
        :return:
        """
        print "> Setup Management Services"
        self._cm.update_config({"TSQUERY_STREAMS_LIMIT": 1000})
        hosts = manager.get_hosts(include_cm_host=True)
        # pick hostId that match the ipAddress of cm_server
        # mgmt_host may be empty then use the 1st host from the -w
        try:
            mgmt_host = [x for x in hosts if x.ipAddress == socket.gethostbyname(cmx.cm_server)][0]
        except IndexError:
            mgmt_host = [x for x in hosts if x.id == 0][0]

        for role_type in [x for x in self._service.get_role_types() if x in self._role_list]:
            try:
                if not [x for x in self._service.get_all_roles() if x.type == role_type]:
                    print "Creating Management Role %s " % role_type
                    role_name = "mgmt-%s-%s" % (role_type, mgmt_host.md5host)
                    for cmd in self._service.create_role(role_name, role_type, mgmt_host.hostId).get_commands():
                        check.status_for_command("Creating %s" % role_name, cmd)
            except ApiException as err:
                print "ERROR: %s " % err.message

        # now configure each role
        for group in [x for x in self._service.get_all_role_config_groups() if x.roleType in self._role_list]:
            if group.roleType == "ACTIVITYMONITOR":
                group.update_config({"firehose_database_host": "%s:7432" % socket.getfqdn(cmx.cm_server),
                                     "firehose_database_user": "amon",
                                     "firehose_database_password": cmx.amon_password,
                                     "firehose_database_type": "postgresql",
                                     "firehose_database_name": "amon",
                                     "firehose_heapsize": "615514112"})
            elif group.roleType == "ALERTPUBLISHER":
                group.update_config({})
            elif group.roleType == "EVENTSERVER":
                group.update_config({"event_server_heapsize": "492830720"})
            elif group.roleType == "HOSTMONITOR":
                group.update_config({"firehose_non_java_memory_bytes": "1610612736",
                                     "firehose_heapsize": "268435456"})
            elif group.roleType == "SERVICEMONITOR":
                group.update_config({"firehose_non_java_memory_bytes": "1610612736",
                                     "firehose_heapsize": "268435456"})
            elif group.roleType == "NAVIGATOR" and manager.licensed():
                group.update_config({"navigator_heapsize": "492830720"})
            elif group.roleType == "NAVIGATORMETASERVER" and manager.licensed():
                group.update_config({"navigator_heapsize": "1232076800"})
            elif group.roleType == "NAVIGATORMETADATASERVER" and manager.licensed():
                group.update_config({})
            elif group.roleType == "REPORTSMANAGER" and manager.licensed():
                group.update_config({"headlamp_database_host": "%s:7432" % socket.getfqdn(cmx.cm_server),
                                     "headlamp_database_name": "rman",
                                     "headlamp_database_password": cmx.rman_password,
                                     "headlamp_database_type": "postgresql",
                                     "headlamp_database_user": "rman",
                                     "headlamp_heapsize": "492830720"})

    @classmethod
    def licensed(cls):
        """
        Check if Cluster is licensed
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        cm = api.get_cloudera_manager()
        try:
            return bool(cm.get_license().uuid)
        except ApiException as err:
            return "Express" not in err.message

    @classmethod
    def upload_license(cls):
        """
        Upload License file
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        cm = api.get_cloudera_manager()
        if cmx.license_file and not manager.licensed():
            print "Upload license"
            with open(cmx.license_file, 'r') as f:
                license_contents = f.read()
                print "Upload CM License: \n %s " % license_contents
                cm.update_license(license_contents)
                # REPORTSMANAGER required after applying license
                manager("REPORTSMANAGER").setup()
                manager("REPORTSMANAGER").start()

    @classmethod
    def begin_trial(cls):
        """
        Begin Trial
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        print "def begin_trial"
        if not manager.licensed():
            try:
                api.post("/cm/trial/begin")
                # REPORTSMANAGER required after applying license
                manager("REPORTSMANAGER").setup()
                # manager("REPORTSMANAGER").start()
            except ApiException as err:
                print err.message

    @classmethod
    def get_mgmt_password(cls, role_type):
        """
        Get password for "ACTIVITYMONITOR', 'REPORTSMANAGER', 'NAVIGATOR"
        :param role_type:
        :return False if db.mgmt.properties is missing
        """
        contents = []
        mgmt_password = False

        if os.path.isfile('/etc/cloudera-scm-server/db.mgmt.properties'):
            try:
                print "> Reading %s password from /etc/cloudera-scm-server/db.mgmt.properties" % role_type
                with open(os.path.join('/etc/cloudera-scm-server', 'db.mgmt.properties')) as f:
                    contents = f.readlines()

                # role_type expected to be in
                # "ACTIVITYMONITOR', 'REPORTSMANAGER', 'NAVIGATOR"
                if role_type in ['ACTIVITYMONITOR', 'REPORTSMANAGER', 'NAVIGATOR']:
                    idx = "com.cloudera.cmf.%s.db.password=" % role_type
                    match = [s.rstrip('\n') for s in contents if idx in s][0]
                    mgmt_password = match[match.index(idx) + len(idx):]

            except IOError:
                print "Unable to open file: /etc/cloudera-scm-server/db.mgmt.properties"

        return mgmt_password

    @classmethod
    def get_hosts(cls, include_cm_host=False):
        """
        because api.get_all_hosts() returns all the hosts as instanceof ApiHost: hostId hostname ipAddress
        and cluster.list_hosts() returns all the cluster hosts as instanceof ApiHostRef: hostId
        we only need Cluster hosts with instanceof ApiHost: hostId hostname ipAddress + md5host
        preserve host order in -w
        hashlib.md5(host.hostname).hexdigest()
        attributes = {'id': None, 'hostId': None, 'hostname': None, 'md5host': None, 'ipAddress': None, }
        return a list of hosts
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)

        w_hosts = set(enumerate(cmx.host_names))
        if include_cm_host and socket.gethostbyname(cmx.cm_server) \
                not in [socket.gethostbyname(x) for x in cmx.host_names]:
            w_hosts.add((len(w_hosts), cmx.cm_server))

        hosts = []
        for idx, host in w_hosts:
            _host = [x for x in api.get_all_hosts() if x.ipAddress == socket.gethostbyname(host)][0]
            hosts.append({
                'id': idx,
                'hostId': _host.hostId,
                'hostname': _host.hostname,
                'md5host': hashlib.md5(_host.hostname).hexdigest(),
                'ipAddress': _host.ipAddress,
            })

        return [type('', (), x) for x in hosts]

    @classmethod
    def restart_management(cls):
        """
        Restart Management Services
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        mgmt = api.get_cloudera_manager().get_service()

        check.status_for_command("Stop Management services", mgmt.stop())
        check.status_for_command("Start Management services", mgmt.start())


class ServiceActions:
    """
    Example stopping/starting services ['HBASE', 'IMPALA', 'SPARK', 'SOLR']
    :param service_list:
    :param action:
    :return:
    """

    def __init__(self, *service_list):
        self._service_list = service_list
        self._api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                                version=cmx.api_version)
        self._cluster = self._api.get_cluster(cmx.cluster_name)

    def stop(self):
        self._action('stop')

    def start(self):
        self._action('start')

    def restart(self):
        self._action('restart')

    def _action(self, action):
        state = {'start': ['STOPPED'], 'stop': ['STARTED'], 'restart': ['STARTED', 'STOPPED']}
        for services in [x for x in self._cluster.get_all_services()
                         if x.type in self._service_list and x.serviceState in state[action]]:
            check.status_for_command("%s service %s" % (action.upper(), services.type),
                                     getattr(self._cluster.get_service(services.name), action)())

    @classmethod
    def get_service_type(cls, name):
        """
        Returns service based on service type name
        :param name:
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        cluster = api.get_cluster(cmx.cluster_name)
        try:
            service = [x for x in cluster.get_all_services() if x.type == name][0]
        except IndexError:
            service = None

        return service

    @classmethod
    def deploy_client_config_for(cls, obj):
        """
        Example deploying GATEWAY Client Config on each host
        Note: only recommended if you need to deploy on a specific hostId.
        Use the cluster.deploy_client_config() for normal use.
        example usage:
        # hostId
        for host in get_cluster_hosts(include_cm_host=True):
            deploy_client_config_for(host.hostId)
        # cdh service
        for service in cluster.get_all_services():
            deploy_client_config_for(service)
        :param host.hostId, or ApiService:
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        # cluster = api.get_cluster(cmx.cluster_name)
        if isinstance(obj, str) or isinstance(obj, unicode):
            for role_name in [x.roleName for x in api.get_host(obj).roleRefs if 'GATEWAY' in x.roleName]:
                service = cdh.get_service_type('GATEWAY')
                print "Deploying client config for service: %s - host: [%s]" % \
                      (service.type, api.get_host(obj).hostname)
                check.status_for_command("Deploy client config for role %s" %
                                         role_name, service.deploy_client_config(role_name))
        elif isinstance(obj, ApiService):
            for role in obj.get_roles_by_type("GATEWAY"):
                check.status_for_command("Deploy client config for role %s" %
                                         role.name, obj.deploy_client_config(role.name))

    @classmethod
    def create_service_role(cls, service, role_type, host):
        """
        Helper function to create a role
        :return:
        """
        service_name = service.name[:4] + hashlib.md5(service.name).hexdigest()[:8] \
            if len(role_type) > 24 else service.name

        role_name = "-".join([service_name, role_type, host.md5host])[:64]
        print "Creating role: %s on host: [%s]" % (role_name, host.hostname)
        if not [role for role in service.get_all_roles() if role_name in role.name]:
            [check.status_for_command("Creating role: %s on host: [%s]" % (role_name, host.hostname), cmd)
             for cmd in service.create_role(role_name, role_type, host.hostId).get_commands()]

    @classmethod
    def restart_cluster(cls):
        """
        Restart Cluster and Cluster wide deploy client config
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        cluster = api.get_cluster(cmx.cluster_name)
        print "Restart cluster: %s" % cmx.cluster_name
        check.status_for_command("Stop %s" % cmx.cluster_name, cluster.stop())
        check.status_for_command("Start %s" % cmx.cluster_name, cluster.start())
        # Example deploying cluster wide Client Config
        check.status_for_command("Deploy client config for %s" % cmx.cluster_name, cluster.deploy_client_config())

    @classmethod
    def dependencies_for(cls, service):
        """
        Utility function returns dict of service dependencies
        :return:
        """
        service_config = {}
        config_types = {"hue_webhdfs": ['NAMENODE', 'HTTPFS'], "hdfs_service": "HDFS", "sentry_service": "SENTRY",
                        "zookeeper_service": "ZOOKEEPER", "hbase_service": "HBASE",
                        "hue_hbase_thrift": "HBASETHRIFTSERVER", "solr_service": "SOLR",
                        "hive_service": "HIVE", "sqoop_service": "SQOOP",
                        "impala_service": "IMPALA", "oozie_service": "OOZIE",
                        "mapreduce_yarn_service": ['MAPREDUCE', 'YARN'], "yarn_service": "YARN"}

        dependency_list = []
        # get required service config
        for k, v in service.get_config(view="full")[0].items():
            if v.required:
                dependency_list.append(k)

        # Extended dependence list, adding the optional ones as well
        if service.type == 'HUE':
            dependency_list.extend(['hbase_service', 'solr_service', 'sqoop_service',
                                    'impala_service', 'hue_hbase_thrift'])
        if service.type in ['HIVE', 'HDFS', 'HUE', 'OOZIE', 'MAPREDUCE', 'YARN', 'ACCUMULO16']:
            dependency_list.append('zookeeper_service')
        if service.type in ['HIVE']:
            dependency_list.append('sentry_service')
        if service.type == 'OOZIE':
            dependency_list.append('hive_service')
        if service.type in ['FLUME', 'IMPALA']:
            dependency_list.append('hbase_service')
        if service.type in ['FLUME', 'SPARK', 'SENTRY', 'ACCUMULO16']:
            dependency_list.append('hdfs_service')
        if service.type == 'FLUME':
            dependency_list.append('solr_service')

        for key in dependency_list:
            if key == "hue_webhdfs":
                hdfs = cdh.get_service_type('HDFS')
                if hdfs is not None:
                    service_config[key] = [x.name for x in hdfs.get_roles_by_type('NAMENODE')][0]
                    # prefer HTTPS over NAMENODE
                    if [x.name for x in hdfs.get_roles_by_type('HTTPFS')]:
                        service_config[key] = [x.name for x in hdfs.get_roles_by_type('HTTPFS')][0]
            elif key == "mapreduce_yarn_service":
                for _type in config_types[key]:
                    if cdh.get_service_type(_type) is not None:
                        service_config[key] = cdh.get_service_type(_type).name
                    # prefer YARN over MAPREDUCE
                    if cdh.get_service_type(_type) is not None and _type == 'YARN':
                        service_config[key] = cdh.get_service_type(_type).name
            elif key == "hue_hbase_thrift":
                hbase = cdh.get_service_type('HBASE')
                if hbase is not None:
                    service_config[key] = [x.name for x in hbase.get_roles_by_type(config_types[key])][0]
            else:
                if cdh.get_service_type(config_types[key]) is not None:
                    service_config[key] = cdh.get_service_type(config_types[key]).name

        return service_config


class ActiveCommands:
    def __init__(self):
        self._api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password,
                                version=initVar.cmx.api_version)

    def status_for_command(self, message, command):
        """
        Helper to check active command status
        :param message:
        :param command:
        :return:
        """
        _state = 0
        _bar = ['[|]', '[/]', '[-]', '[\\]']
        while True:
            if self._api.get("/commands/%s" % command.id)['active']:
                sys.stdout.write(_bar[_state % 4] + ' ' + message + ' ' + ('\b' * (len(message) + 5)))
                sys.stdout.flush()
                _state += 1
                time.sleep(0.5)
            else:
                print "\n [%s] %s" % (command.id, self._api.get("/commands/%s" % command.id)['resultMessage'])
                self._child_cmd(self._api.get("/commands/%s" % command.id)['children']['items'])
                break

    def _child_cmd(self, cmd):
        """
        Helper cmd has child objects
        :param cmd:
        :return:
        """
        if len(cmd) != 0:
            print " Sub tasks result(s):"
            for resMsg in cmd:
                if resMsg.get('resultMessage'):
                    print "  [%s] %s" % (resMsg['id'], resMsg['resultMessage']) if not resMsg.get('roleRef') \
                        else "  [%s] %s - %s" % (resMsg['id'], resMsg['resultMessage'], resMsg['roleRef']['roleName'])
                self._child_cmd(self._api.get("/commands/%s" % resMsg['id'])['children']['items'])
