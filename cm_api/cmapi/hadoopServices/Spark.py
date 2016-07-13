#!/usr/bin/env python
import random

from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def sparkSetup():
    """
    Spark
    > Execute command CreateSparkUserDirCommand on service Spark
    > Execute command CreateSparkHistoryDirCommand on service Spark
    > Execute command SparkUploadJarServiceCommand on service Spark
    Starting Spark Service
    :return:
    """
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "SPARK"
    if initVar.cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "spark"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = initVar.manager.get_hosts()

        # Service-Wide
        service.update_config(initVar.cdh.dependencies_for(service))

        initVar.cdh.create_service_role(service, "SPARK_MASTER", [x for x in hosts if x.id == 0][0])
        initVar.cdh.create_service_role(service, "SPARK_HISTORY_SERVER", random.choice(hosts))

        for role_type in ['GATEWAY', 'SPARK_WORKER']:
            for host in initVar.manager.get_hosts(include_cm_host=(role_type == 'GATEWAY')):
                initVar.cdh.create_service_role(service, role_type, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # initVar.cdh.deploy_client_config_for(service)

        initVar.check.status_for_command("Execute command CreateSparkUserDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkUserDirCommand'))
        initVar.check.status_for_command("Execute command CreateSparkHistoryDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkHistoryDirCommand'))
        initVar.check.status_for_command("Execute command SparkUploadJarServiceCommand on service Spark",
                                 service.service_command_by_name('SparkUploadJarServiceCommand'))

        # This service is started later on
        # initVar.check.status_for_command("Starting Spark Service", service.start())


def sparkonyarnSetup():
    """
    Sqoop Client
    :return:
    """
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "SPARK_ON_YARN"
    if initVar.cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "spark_on_yarn"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = initVar.manager.get_hosts()

        # Service-Wide
        service.update_config(initVar.cdh.dependencies_for(service))
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "SPARK_YARN_HISTORY_SERVER":
                rcg.update_config({"history_server_max_heapsize": "153092096"})

        initVar.cdh.create_service_role(service, "SPARK_YARN_HISTORY_SERVER", random.choice(hosts))

        for host in initVar.manager.get_hosts(include_cm_host=True):
            initVar.cdh.create_service_role(service, "GATEWAY", host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # initVar.cdh.deploy_client_config_for(service)

        initVar.check.status_for_command("Execute command CreateSparkUserDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkUserDirCommand'))
        initVar.check.status_for_command("Execute command CreateSparkHistoryDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkHistoryDirCommand'))
        initVar.check.status_for_command("Execute command SparkUploadJarServiceCommand on service Spark",
                                 service.service_command_by_name('SparkUploadJarServiceCommand'))

        # This service is started later on
        # initVar.check.status_for_command("Starting Spark Service", service.start())
