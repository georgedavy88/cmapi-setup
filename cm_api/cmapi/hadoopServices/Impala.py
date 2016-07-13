#!/usr/bin/env python
import random

from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def impalaSetup(enable_llama=False):
    """
    Impala
    > Creating Impala user directory
    Starting Impala Service
    :param enable_llama:
    :return:
    """
    api = ApiResource(server_host=api.cm_server, username=api.username, password=api.password, version=api.api_version)
    cluster = api.get_cluster(api.cluster_name)
    service_type = "IMPALA"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "impala"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "IMPALAD":
                rcg.update_config({"impalad_memory_limit": "618659840",
                                   "enable_audit_event_log": True,
                                   "scratch_dirs": "/data/impala/impalad"})

        for role_type in ['CATALOGSERVER', 'STATESTORE']:
            cdh.create_service_role(service, role_type, random.choice(hosts))

        # Install ImpalaD
        for host in hosts:
            cdh.create_service_role(service, "IMPALAD", host)

        check.status_for_command("Creating Impala user directory", service.create_impala_user_dir())
        # Impala will be started/stopped when we enable_llama_rm
        # This service is started later on
        # check.status_for_command("Starting Impala Service", service.start())

        # Enable YARN and Impala Integrated Resource Management
        # http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/admin_llama.html
        yarn = cdh.get_service_type('YARN')
        if yarn is not None and enable_llama is True:
            # enable cgroup-based resource management for all hosts with NodeManager roles.
            cm = api.get_cloudera_manager()
            cm.update_all_hosts_config({"rm_enabled": True})
            yarn.update_config({"yarn_service_cgroups": True, "yarn_service_lce_always": True})
            role_group = yarn.get_role_config_group("%s-RESOURCEMANAGER-BASE" % yarn.name)
            role_group.update_config({"yarn_scheduler_minimum_allocation_mb": 0,
                                      "yarn_scheduler_minimum_allocation_vcores": 0})
            check.status_for_command("Enable YARN and Impala Integrated Resource Management",
                                     service.enable_llama_rm(random.choice(hosts).hostId))
