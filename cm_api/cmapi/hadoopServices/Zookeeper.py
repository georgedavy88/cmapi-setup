#!/usr/bin/env python
import random
import ConfigParser
import initVar
from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def zookeeperSetup():
    """
    Zookeeper
    > Waiting for ZooKeeper Service to initialize
    Starting ZooKeeper Service
    :return:
    """
    CONFIG = ConfigParser.ConfigParser()
    CONFIG.read("cm_config.ini")
    ZOOKEEPER_HOSTS = CONFIG.get("SERVICE", "service.zookeeper.hosts").split(',')
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "ZOOKEEPER"
    if initVar.cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "zookeeper"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = initVar.manager.get_zookeeper_hosts()
        service.update_config({"zookeeper_datadir_autocreate": False})

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "SERVER":
                rcg.update_config({"maxClientCnxns": "1024", "zookeeper_server_java_heapsize": "492830720"})
                # Pick 3 hosts and deploy Zookeeper Server role
                for host in random.sample(hosts, 3 if len(hosts) >= 3 else 1):
                    initVar.cdh.create_service_role(service, rcg.roleType, host)

        # init_zookeeper not required as the API performs this when adding Zookeeper
        # initVar.check.status_for_command("Waiting for ZooKeeper Service to initialize", service.init_zookeeper())
        initVar.check.status_for_command("Starting ZooKeeper Service", service.start())
