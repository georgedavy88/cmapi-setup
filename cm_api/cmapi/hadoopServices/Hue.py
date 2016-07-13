#!/usr/bin/env python

from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def hueSetup():
    """
    Hue
    Starting Hue Service
    :return:
    """
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "HUE"
    if initVar.cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "hue"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = initVar.manager.get_hosts()

        # Service-Wide
        service.update_config(initVar.cdh.dependencies_for(service))

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "HUE_SERVER":
                rcg.update_config({})
                initVar.cdh.create_service_role(service, "HUE_SERVER", [x for x in hosts if x.id == 0][0])
                # This service is started later on
                # initVar.check.status_for_command("Starting Hue Service", service.start())
