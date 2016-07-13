#!/usr/bin/env python
from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def setup_flume():
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "FLUME"
    if cdh.get_service_type(service_type) is None:
        service_name = "flume"
        cluster.create_service(service_name.lower(), service_type)
        service = cluster.get_service(service_name)

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))
        hosts = manager.get_hosts()
        cdh.create_service_role(service, "AGENT", [x for x in hosts if x.id == 0][0])
        # This service is started later on
        # check.status_for_command("Starting Flume Agent", service.start())
