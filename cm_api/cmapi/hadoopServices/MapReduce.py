#!/usr/bin/env python
from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def mapreduceSetup():
    """
    MapReduce
    :return:
    """
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "MAPREDUCE"
    if initVar.cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "mapreduce"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = initVar.manager.get_hosts()

        # Service-Wide
        service.update_config(initVar.cdh.dependencies_for(service))

        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "JOBTRACKER":
                # mapreduce-JOBTRACKER - Default Group
                rcg.update_config({"jobtracker_mapred_local_dir_list": "/data/mapred/jt",
                                   "jobtracker_java_heapsize": "492830720",
                                   "mapred_job_tracker_handler_count": "22"})
                initVar.cdh.create_service_role(service, rcg.roleType, [x for x in hosts if x.id == 0][0])
            if rcg.roleType == "TASKTRACKER":
                # mapreduce-TASKTRACKER - Default Group
                rcg.update_config({"tasktracker_mapred_local_dir_list": "/data/mapred/local",
                                   "mapred_tasktracker_map_tasks_maximum": "1",
                                   "mapred_tasktracker_reduce_tasks_maximum": "1",
                                   "task_tracker_java_heapsize": "127926272"})
            if rcg.roleType == "GATEWAY":
                # mapreduce-GATEWAY - Default Group
                rcg.update_config({"mapred_reduce_tasks": "1", "mapred_submit_replication": "1"})

        for role_type in ['GATEWAY', 'TASKTRACKER']:
            for host in initVar.manager.get_hosts(include_cm_host=(role_type == 'GATEWAY')):
                initVar.cdh.create_service_role(service, role_type, host)

                # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
                # initVar.cdh.deploy_client_config_for(service)

                # This service is started later on
                # initVar.check.status_for_command("Starting MapReduce Service", service.start())
