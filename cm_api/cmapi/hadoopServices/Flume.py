#!/usr/bin/env python
from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def flumeSetup():
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "FLUME"
    if initVar.cdh.get_service_type(service_type) is None:
        service_name = "flume"
        cluster.create_service(service_name.lower(), service_type)
        service = cluster.get_service(service_name)

        # Service-Wide
        service.update_config(initVar.cdh.dependencies_for(service))
        hosts = initVar.manager.get_hosts()
        initVar.cdh.create_service_role(service, "AGENT", [x for x in hosts if x.id == 0][0])
        # This service is started later on
        # initVar.check.status_for_command("Starting Flume Agent", service.start())
