#!/usr/bin/env python
from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def sqoopSetup():
    """
    Sqoop 2
    > Creating Sqoop 2 user directory
    > Creating Sqoop 2 Database
    Starting Sqoop 2 Service
    :return:
    """
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "SQOOP"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "sqoop"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = initVar.manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "SQOOP_SERVER":
                rcg.update_config({"sqoop_java_heapsize": "492830720"})

        cdh.create_service_role(service, "SQOOP_SERVER", [x for x in hosts if x.id == 0][0])

        initVar.check.status_for_command("Creating Sqoop 2 user directory", service.create_sqoop_user_dir())
        # CDH Version check if greater than 5.3.0
        vc = lambda v: tuple(map(int, (v.split("."))))
        if vc(initVar.cmx.parcel[0]['version'].split('-')[0]) >= vc("5.3.0"):
            initVar.check.status_for_command("Creating Sqoop 2 Database", service._cmd('SqoopCreateDatabase'))
            # This service is started later on
            # initVar.check.status_for_command("Starting Sqoop 2 Service", service.start())


def sqoopclientSetup():
    """
    Sqoop Client
    :return:
    """
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "SQOOP_CLIENT"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "sqoop_client"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        # hosts = get_cluster_hosts()

        # Service-Wide
        service.update_config({})

        for host in initVar.manager.get_hosts(include_cm_host=True):
            cdh.create_service_role(service, "GATEWAY", host)

            # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
            # cdh.deploy_client_config_for(service)
