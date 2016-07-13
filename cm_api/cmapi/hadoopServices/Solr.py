#!/usr/bin/env python
from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def solrSetup():
    """
    Solr
    > Initializing Solr in ZooKeeper
    > Creating HDFS home directory for Solr
    Starting Solr Service
    :return:
    """
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "SOLR"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "solr"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = initVar.manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "SOLR_SERVER":
                cdh.create_service_role(service, rcg.roleType, [x for x in hosts if x.id == 0][0])
            if rcg.roleType == "GATEWAY":
                for host in initVar.manager.get_hosts(include_cm_host=True):
                    cdh.create_service_role(service, rcg.roleType, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        # check.status_for_command("Initializing Solr in ZooKeeper", service._cmd('initSolr'))
        # check.status_for_command("Creating HDFS home directory for Solr", service._cmd('createSolrHdfsHomeDir'))
        check.status_for_command("Initializing Solr in ZooKeeper", service.init_solr())
        check.status_for_command("Creating HDFS home directory for Solr",
                                 service.create_solr_hdfs_home_dir())
        # This service is started later on
        # check.status_for_command("Starting Solr Service", service.start())
