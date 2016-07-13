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
    api = ApiResource(server_host=api.cm_server, username=api.username, password=api.password, version=api.api_version)
    cluster = api.get_cluster(api.cluster_name)
    service_type = "SPARK"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "spark"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        cdh.create_service_role(service, "SPARK_MASTER", [x for x in hosts if x.id == 0][0])
        cdh.create_service_role(service, "SPARK_HISTORY_SERVER", random.choice(hosts))

        for role_type in ['GATEWAY', 'SPARK_WORKER']:
            for host in manager.get_hosts(include_cm_host=(role_type == 'GATEWAY')):
                cdh.create_service_role(service, role_type, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        check.status_for_command("Execute command CreateSparkUserDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkUserDirCommand'))
        check.status_for_command("Execute command CreateSparkHistoryDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkHistoryDirCommand'))
        check.status_for_command("Execute command SparkUploadJarServiceCommand on service Spark",
                                 service.service_command_by_name('SparkUploadJarServiceCommand'))

        # This service is started later on
        # check.status_for_command("Starting Spark Service", service.start())


def sparkonyarnSetup():
    """
    Sqoop Client
    :return:
    """
    api = ApiResource(server_host=api.cm_server, username=api.username, password=api.password, version=api.api_version)
    cluster = api.get_cluster(api.cluster_name)
    service_type = "SPARK_ON_YARN"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "spark_on_yarn"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "SPARK_YARN_HISTORY_SERVER":
                rcg.update_config({"history_server_max_heapsize": "153092096"})

        cdh.create_service_role(service, "SPARK_YARN_HISTORY_SERVER", random.choice(hosts))

        for host in manager.get_hosts(include_cm_host=True):
            cdh.create_service_role(service, "GATEWAY", host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        check.status_for_command("Execute command CreateSparkUserDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkUserDirCommand'))
        check.status_for_command("Execute command CreateSparkHistoryDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkHistoryDirCommand'))
        check.status_for_command("Execute command SparkUploadJarServiceCommand on service Spark",
                                 service.service_command_by_name('SparkUploadJarServiceCommand'))

        # This service is started later on
        # check.status_for_command("Starting Spark Service", service.start())
