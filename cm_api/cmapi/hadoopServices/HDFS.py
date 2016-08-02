#!/usr/bin/env python
import random
import initVar
import socket
import ConfigParser
from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def hdfsSetup():
    """
    HDFS
    > Checking if the name directories of the NameNode are empty. Formatting HDFS only if empty.
    Starting HDFS Service
    > Creating HDFS /tmp directory
    :return:
    """
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    service_type = "HDFS"
    CONFIG = ConfigParser.ConfigParser()
    CONFIG.read("cm_config.ini")
    if initVar.cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "hdfs"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = initVar.manager.get_hosts()

        # Service-Wide
        service_config = initVar.cdh.dependencies_for(service)
        service_config.update({"dfs_replication": "3",
                               "dfs_block_local_path_access_user": "impala,hbase,mapred,spark"})
        service.update_config(service_config)

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "NAMENODE":
                # hdfs-NAMENODE - Default Group
                rcg.update_config({"dfs_name_dir_list": "/data/dfs/nn",
                                   "namenode_java_heapsize": "1073741824",
                                   "dfs_namenode_handler_count": "30",
                                   "dfs_namenode_service_handler_count": "30",
                                   "dfs_namenode_servicerpc_address": "8022"})
                primary_nn = [x for x in hosts if x.ipAddress == socket.gethostbyname(CONFIG.get("SERVICE", "service.namenode.host"))][0]
                #initVar.cdh.create_service_role(service, rcg.roleType, [x for x in hosts if x.id == 0][0])
                initVar.cdh.create_service_role(service, rcg.roleType, primary_nn)
            if rcg.roleType == "SECONDARYNAMENODE":
                # hdfs-SECONDARYNAMENODE - Default Group
                rcg.update_config({"fs_checkpoint_dir_list": "/data/dfs/snn",
                                   "secondary_namenode_java_heapsize": "1073741824"})
                # chose a server that it's not NN, easier to enable HDFS-HA later
                secondary_nn = [x for x in hosts if x.ipAddress == socket.gethostbyname(CONFIG.get("SERVICE", "service.secondary.namenode.host"))][0]

                initVar.cdh.create_service_role(service, rcg.roleType, secondary_nn)

            if rcg.roleType == "DATANODE":
                # hdfs-DATANODE - Default Group
                rcg.update_config({"datanode_java_heapsize": "127926272",
                                   "dfs_data_dir_list": "/data/dfs/dn",
                                   "dfs_datanode_data_dir_perm": "755",
                                   "dfs_datanode_du_reserved": "3218866585",
                                   "dfs_datanode_failed_volumes_tolerated": "0",
                                   "dfs_datanode_max_locked_memory": "316669952", })

                dnode_hosts = [x for x in hosts if x.ipAddress == socket.gethostbyname(set(enumerate(CONFIG.get("SERVICE", "service.datanode.hosts"))))][0]
                for dnode in dnode_hosts:
                    initVar.cdh.create_service_role(service, role_type, dnode)

            if rcg.roleType == "BALANCER":
                # hdfs-BALANCER - Default Group
                rcg.update_config({"balancer_java_heapsize": "492830720"})
            if rcg.roleType == "GATEWAY":
                # hdfs-GATEWAY - Default Group
                rcg.update_config({"dfs_client_use_trash": True})
                gateway_hosts = [x for x in hosts if x.ipAddress == socket.gethostbyname(set(enumerate(CONFIG.get("SERVICE", "service.gateway.hosts"))))][0]
                for gnode in gateway_hosts:
                    initVar.cdh.create_service_role(service, role_type, gnodes)

        #for role_type in ['DATANODE', 'GATEWAY']:
        #    for host in initVar.manager.get_hosts(include_cm_host=(role_type == 'GATEWAY')):
        #        initVar.cdh.create_service_role(service, role_type, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # initVar.cdh.deploy_client_config_for(service)

        nn_role_type = service.get_roles_by_type("NAMENODE")[0]
        commands = service.format_hdfs(nn_role_type.name)
        for cmd in commands:
            initVar.check.status_for_command("Format NameNode", cmd)

        initVar.check.status_for_command("Starting HDFS.", service.start())
        initVar.check.status_for_command("Creating HDFS /tmp directory", service.create_hdfs_tmp())
