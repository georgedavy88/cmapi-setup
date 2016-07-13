#!/usr/bin/env python
from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def oozieSetup():
    """
    Oozie
    > Creating Oozie database
    > Installing Oozie ShareLib in HDFS
    Starting Oozie Service
    :return:
    """
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "OOZIE"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "oozie"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = initVar.manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "OOZIE_SERVER":
                rcg.update_config({"oozie_java_heapsize": "492830720"})
                cdh.create_service_role(service, rcg.roleType, [x for x in hosts if x.id == 0][0])

        initVar.check.status_for_command("Creating Oozie database", service.create_oozie_db())
        initVar.check.status_for_command("Installing Oozie ShareLib in HDFS", service.install_oozie_sharelib())
        # This service is started later on
        # initVar.check.status_for_command("Starting Oozie Service", service.start())
