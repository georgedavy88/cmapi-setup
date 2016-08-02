#!/usr/bin/env python

def setup_sentry():
    api = ApiResource(server_host=api.cm_server, username=api.username, password=api.password, version=api.api_version)
    cluster = api.get_cluster(api.cluster_name)
    service_type = "SENTRY"
    if cdh.get_service_type(service_type) is None:
        service_name = "sentry"
        cluster.create_service(service_name.lower(), service_type)
        service = cluster.get_service(service_name)

        # Service-Wide
        # sentry_server_database_host: Assuming embedded DB is running from where embedded-db is located.
        service_config = {"sentry_server_database_host": socket.getfqdn(api.cm_server),
                          "sentry_server_database_user": "sentry",
                          "sentry_server_database_name": "sentry",
                          "sentry_server_database_password": "cloudera",
                          "sentry_server_database_port": "7432",
                          "sentry_server_database_type": "postgresql"}

        service_config.update(cdh.dependencies_for(service))
        service.update_config(service_config)
        hosts = manager.get_hosts()

        cdh.create_service_role(service, "SENTRY_SERVER", random.choice(hosts))
        check.status_for_command("Creating Sentry Database Tables", service.create_sentry_database_tables())

        # Update configuration for Hive service
        hive = cdh.get_service_type('HIVE')
        hive.update_config(cdh.dependencies_for(hive))

        # Disable HiveServer2 Impersonation - hive-HIVESERVER2-BASE - Default Group
        role_group = hive.get_role_config_group("%s-HIVESERVER2-BASE" % hive.name)
        role_group.update_config({"hiveserver2_enable_impersonation": False})

        # This service is started later on
        # check.status_for_command("Starting Sentry Server", service.start())
