#!/usr/bin/env python

def enable_kerberos():
    """
    Enable Kerberos
    > Import KDC Account Manager Credentials
    > Generate Credentials
    > Stop cluster
    > Stop Cloudera Management Services
    > Configure all services to use Kerberos
    > Wait for credentials to be generated
    > Deploy client configuration
    > Start Cloudera Management Services
    > Start cluster
    :return:
    """
    api = ApiResource(server_host=api.cm_server, username=api.username, password=api.password, version=api.api_version)
    cm = api.get_cloudera_manager()
    cluster = api.get_cluster(api.cluster_name)
    print "> Setup Kerberos"
    cm.update_config({"KDC_HOST": api.kerberos['kdc_host'],
                      "SECURITY_REALM": api.kerberos['security_realm']})

    if api.api_version >= 11:
        check.status_for_command("Configure Kerberos for Cluster",
                                 cluster.configure_for_kerberos(datanode_transceiver_port=1004,
                                                                datanode_web_port=1006))
        check.status_for_command("Stop Cloudera Management Services", cm.get_service().stop())
        # check.status_for_command("Wait for credentials to be generated", cm.generate_credentials())
        check.status_for_command("Start Cloudera Management Services", cm.get_service().start())
    else:
        hdfs = cdh.get_service_type('HDFS')
        zookeeper = cdh.get_service_type('ZOOKEEPER')
        hue = cdh.get_service_type('HUE')
        hosts = manager.get_hosts()

        check.status_for_command("Import Admin Credentials",
                                 cm.import_admin_credentials(username=str(api.kerberos['kdc_user']),
                                                             password=str(api.kerberos['kdc_password'])))
        check.status_for_command("Wait for credentials to be generated", cm.generate_credentials())
        time.sleep(10)
        check.status_for_command("Stop cluster: %s" % api.cluster_name, cluster.stop())
        check.status_for_command("Stop Cloudera Management Services", cm.get_service().stop())

        # Configure all services to use MIT Kerberos
        # HDFS Service-Wide
        hdfs.update_config({"hadoop_security_authentication": "kerberos", "hadoop_security_authorization": True})

        # hdfs-DATANODE-BASE - Default Group
        role_group = hdfs.get_role_config_group("%s-DATANODE-BASE" % hdfs.name)
        role_group.update_config({"dfs_datanode_http_port": "1006", "dfs_datanode_port": "1004",
                                  "dfs_datanode_data_dir_perm": "700"})

        # Zookeeper Service-Wide
        zookeeper.update_config({"enableSecurity": True})
        cdh.create_service_role(hue, "KT_RENEWER", [x for x in hosts if x.id == 0][0])

        # Example deploying cluster wide Client Config
        check.status_for_command("Deploy client config for %s" % api.cluster_name, cluster.deploy_client_config())
        check.status_for_command("Start Cloudera Management Services", cm.get_service().start())
        # check.status_for_command("Start cluster: %s" % api.cluster_name, cluster.start())


def disable_kerberos():
    """
    Disable Kerberos
    > Stop cluster
    > Stop Cloudera Management Services
    > Configure all services to not use Kerberos
    > Deploy client configuration
    > Start Cloudera Management Services
    > Start cluster
    :return:
    """
    api = ApiResource(server_host=api.cm_server, username=api.username, password=api.password, version=api.api_version)
    cm = api.get_cloudera_manager()
    cluster = api.get_cluster(api.cluster_name)
    print "> Setup Kerberos"
    cm.update_config({"KDC_HOST": None, "SECURITY_REALM": None})
    hdfs = cdh.get_service_type('HDFS')
    zookeeper = cdh.get_service_type('ZOOKEEPER')
    hue = cdh.get_service_type('HUE')

    check.status_for_command("Stop cluster: %s" % api.cluster_name, cluster.stop())
    check.status_for_command("Stop Cloudera Management Services", cm.get_service().stop())

    # Configure all services to use simple authentication
    # HDFS Service-Wide
    hdfs.update_config({"hadoop_security_authentication": "simple", "hadoop_security_authorization": False})

    # hdfs-DATANODE-BASE - Default Group
    role_group = hdfs.get_role_config_group("%s-DATANODE-BASE" % hdfs.name)
    role_group.update_config({"dfs_datanode_http_port": "50075", "dfs_datanode_port": "50010",
                              "dfs_datanode_data_dir_perm": "700"})

    # Zookeeper Service-Wide
    zookeeper.update_config({"enableSecurity": False})
    kt_renewer_role = hue.get_roles_by_type("HUE_SERVER")[0].name
    check.status_for_command("Delete KT_RENEWER role: %s" % kt_renewer_role, hue.delete_role(kt_renewer_role))

    # Example deploying cluster wide Client Config
    check.status_for_command("Deploy client config for %s" % api.cluster_name, cluster.deploy_client_config())
    check.status_for_command("Start Cloudera Management Services", cm.get_service().start())
    check.status_for_command("Start cluster: %s" % api.cluster_name, cluster.start())
