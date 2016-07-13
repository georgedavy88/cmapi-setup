#!/usr/bin/env python
import socket
import random

from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def hiveSetup():
    """
    Hive
    > Creating Hive Metastore Database
    > Creating Hive Metastore Database Tables
    > Creating Hive user directory
    > Creating Hive warehouse directory
    Starting Hive Service
    :return:
    """
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "HIVE"
    if initVar.cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "hive"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = initVar.manager.get_hosts()

        # Service-Wide
        # hive_metastore_database_host: Assuming embedded DB is running from where embedded-db is located.
        service_config = {"hive_metastore_database_host": socket.getfqdn(initVar.cmx.cm_server),
                          "hive_metastore_database_user": "hive",
                          "hive_metastore_database_name": "hive",
                          "hive_metastore_database_password": "cloudera",
                          "hive_metastore_database_port": "7432",
                          "hive_metastore_database_type": "postgresql"}
        service_config.update(initVar.cdh.dependencies_for(service))
        service.update_config(service_config)

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "HIVEMETASTORE":
                rcg.update_config({"hive_metastore_java_heapsize": "492830720"})
            if rcg.roleType == "HIVESERVER2":
                rcg.update_config({"hiveserver2_java_heapsize": "144703488"})

        for role_type in ['HIVEMETASTORE', 'HIVESERVER2']:
            initVar.cdh.create_service_role(service, role_type, random.choice(hosts))

        for host in initVar.manager.get_hosts(include_cm_host=True):
            initVar.cdh.create_service_role(service, "GATEWAY", host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # initVar.cdh.deploy_client_config_for(service)

        initVar.check.status_for_command("Creating Hive Metastore Database Tables", service.create_hive_metastore_tables())
        initVar.check.status_for_command("Creating Hive user directory", service.create_hive_userdir())
        initVar.check.status_for_command("Creating Hive warehouse directory", service.create_hive_warehouse())
        # This service is started later on
        # initVar.check.status_for_command("Starting Hive Service", service.start())
