#!/usr/bin/env python
import random

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
    apiR = ApiResource(server_host=api.cm_server, username=api.username, password=api.password, version=api.api_version)
    cluster = apiR.get_cluster(api.cluster_name)
    service_type = "HDFS"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "hdfs"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service_config = cdh.dependencies_for(service)
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
                cdh.create_service_role(service, rcg.roleType, [x for x in hosts if x.id == 0][0])
            if rcg.roleType == "SECONDARYNAMENODE":
                # hdfs-SECONDARYNAMENODE - Default Group
                rcg.update_config({"fs_checkpoint_dir_list": "/data/dfs/snn",
                                   "secondary_namenode_java_heapsize": "1073741824"})
                # chose a server that it's not NN, easier to enable HDFS-HA later
                secondary_nn = random.choice([host for host in hosts if host.hostId not in
                                              [x.hostRef.hostId for x in service.get_roles_by_type("NAMENODE")]]) \
                    if len(hosts) > 1 else random.choice(hosts)

                cdh.create_service_role(service, rcg.roleType, secondary_nn)

            if rcg.roleType == "DATANODE":
                # hdfs-DATANODE - Default Group
                rcg.update_config({"datanode_java_heapsize": "127926272",
                                   "dfs_data_dir_list": "/data/dfs/dn",
                                   "dfs_datanode_data_dir_perm": "755",
                                   "dfs_datanode_du_reserved": "3218866585",
                                   "dfs_datanode_failed_volumes_tolerated": "0",
                                   "dfs_datanode_max_locked_memory": "316669952", })
            if rcg.roleType == "BALANCER":
                # hdfs-BALANCER - Default Group
                rcg.update_config({"balancer_java_heapsize": "492830720"})
            if rcg.roleType == "GATEWAY":
                # hdfs-GATEWAY - Default Group
                rcg.update_config({"dfs_client_use_trash": True})

        for role_type in ['DATANODE', 'GATEWAY']:
            for host in manager.get_hosts(include_cm_host=(role_type == 'GATEWAY')):
                cdh.create_service_role(service, role_type, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        nn_role_type = service.get_roles_by_type("NAMENODE")[0]
        commands = service.format_hdfs(nn_role_type.name)
        for cmd in commands:
            check.status_for_command("Format NameNode", cmd)

        check.status_for_command("Starting HDFS.", service.start())
        check.status_for_command("Creating HDFS /tmp directory", service.create_hdfs_tmp())
