#!/usr/bin/env python
import random

from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def hbaseSetup():
    """
    HBase
    > Creating HBase root directory
    Starting HBase Service
    :return:
    """
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "HBASE"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "hbase"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = initVar.manager.get_hosts()

        # Service-Wide
        service_config = {"hbase_enable_indexing": True, "hbase_enable_replication": True,
                          "zookeeper_session_timeout": "30000"}
        service_config.update(cdh.dependencies_for(service))
        service.update_config(service_config)

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "MASTER":
                rcg.update_config({"hbase_master_java_heapsize": "492830720"})
            if rcg.roleType == "REGIONSERVER":
                rcg.update_config({"hbase_regionserver_java_heapsize": "365953024",
                                   "hbase_regionserver_java_opts": "-XX:+UseParNewGC -XX:+UseConcMarkSweepGC "
                                                                   "-XX:-CMSConcurrentMTEnabled "
                                                                   "-XX:CMSInitiatingOccupancyFraction=70 "
                                                                   "-XX:+CMSParallelRemarkEnabled -verbose:gc "
                                                                   "-XX:+PrintGCDetails -XX:+PrintGCDateStamps"})

        for role_type in ['MASTER', 'HBASETHRIFTSERVER', 'HBASERESTSERVER']:
            cdh.create_service_role(service, role_type, random.choice(hosts))

        for role_type in ['GATEWAY', 'REGIONSERVER']:
            for host in initVar.manager.get_hosts(include_cm_host=(role_type == 'GATEWAY')):
                cdh.create_service_role(service, role_type, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        check.status_for_command("Creating HBase root directory", service.create_hbase_root())
        # This service is started later on
        # check.status_for_command("Starting HBase Service", service.start())
