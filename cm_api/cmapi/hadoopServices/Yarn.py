#!/usr/bin/env python
import random

from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def setup_yarn():
    """
    Yarn
    > Creating MR2 job history directory
    > Creating NodeManager remote application log directory
    Starting YARN (MR2 Included) Service
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "YARN"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "yarn"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "RESOURCEMANAGER":
                # yarn-RESOURCEMANAGER - Default Group
                rcg.update_config({"resource_manager_java_heapsize": "492830720",
                                   "yarn_scheduler_maximum_allocation_mb": "2568",
                                   "yarn_scheduler_maximum_allocation_vcores": "2"})
                cdh.create_service_role(service, rcg.roleType, [x for x in hosts if x.id == 0][0])
            if rcg.roleType == "JOBHISTORY":
                # yarn-JOBHISTORY - Default Group
                rcg.update_config({"mr2_jobhistory_java_heapsize": "492830720"})
                cdh.create_service_role(service, rcg.roleType, random.choice(hosts))
            if rcg.roleType == "NODEMANAGER":
                # yarn-NODEMANAGER - Default Group
                rcg.update_config({"yarn_nodemanager_heartbeat_interval_ms": "100",
                                   "yarn_nodemanager_local_dirs": "/data/yarn/nm",
                                   "yarn_nodemanager_resource_cpu_vcores": "2",
                                   "yarn_nodemanager_resource_memory_mb": "2568",
                                   "node_manager_java_heapsize": "127926272"})
                for host in hosts:
                    cdh.create_service_role(service, rcg.roleType, host)
            if rcg.roleType == "GATEWAY":
                # yarn-GATEWAY - Default Group
                rcg.update_config({"mapred_reduce_tasks": "505413632", "mapred_submit_replication": "1",
                                   "mapred_reduce_tasks": "3"})
                for host in manager.get_hosts(include_cm_host=True):
                    cdh.create_service_role(service, rcg.roleType, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        check.status_for_command("Creating MR2 job history directory", service.create_yarn_job_history_dir())
        check.status_for_command("Creating NodeManager remote application log directory",
                                 service.create_yarn_node_manager_remote_app_log_dir())
        # This service is started later on
        # check.status_for_command("Starting YARN (MR2 Included) Service", service.start())
