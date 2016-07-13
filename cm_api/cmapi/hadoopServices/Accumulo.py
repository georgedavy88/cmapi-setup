#!/usr/bin/env python

import random

from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def accumuloSetup():
    """
    Accumulo 1.6
    > Deploy Client Configuration
    > Create Accumulo Home Dir on service Accumulo 1.6
    > Create Accumulo User Dir on service Accumulo 1.6
    > Initialize Accumulo on service Accumulo 1.6
    Start Accumulo 1.6
    :return:
    """
    api = ApiResource(server_host=api.cm_server, username=api.username, password=api.password, version=api.api_version)
    cluster = api.get_cluster(api.cluster_name)
    service_type = "ACCUMULO16"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "accumulo16"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Deploy ACCUMULO16 Parcel
        parcel = [x for x in cluster.get_all_parcels() if x.product == 'ACCUMULO' and
                  'cdh5' in x.version][0]

        accumulo_parcel = {'product': str(parcel.product.upper()), 'version': str(parcel.version).lower()}
        print "> Parcel action for parcel: [ %s-%s ]" % (parcel.product, parcel.version)
        cluster_parcel = cluster.get_parcel(product=parcel.product, version=parcel.version)
        if "ACTIVATED" not in cluster_parcel.stage:
            parcel_action(parcel_item=accumulo_parcel, function="start_removal_of_distribution",
                          expected_stage=['DOWNLOADED', 'AVAILABLE_REMOTELY', 'ACTIVATING'],
                          action_description="Un-Distribute Parcel")
            parcel_action(parcel_item=accumulo_parcel, function="start_download",
                          expected_stage=['DOWNLOADED'], action_description="Download Parcel")
            parcel_action(parcel_item=accumulo_parcel, function="start_distribution", expected_stage=['DISTRIBUTED'],
                          action_description="Distribute Parcel")
            parcel_action(parcel_item=accumulo_parcel, function="activate", expected_stage=['ACTIVATED'],
                          action_description="Activate Parcel")

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Create Accumulo roles
        for role_type in ['ACCUMULO16_MASTER', 'ACCUMULO16_TRACER', 'ACCUMULO16_GC',
                          'ACCUMULO16_TSERVER', 'ACCUMULO16_MONITOR']:
            cdh.create_service_role(service, role_type, random.choice(hosts))

        # Create Accumulo gateway roles
        for host in manager.get_hosts(include_cm_host=True):
            cdh.create_service_role(service, 'GATEWAY', host)

        print "Deploy Client Configuration"
        cluster.deploy_client_config()
        check.status_for_command("Execute command Create Accumulo Home Dir on service Accumulo 1.6",
                                 service.service_command_by_name('CreateHdfsDirCommand'))
        check.status_for_command("Execute command Create Accumulo User Dir on service Accumulo 1.6",
                                 service.service_command_by_name('CreateAccumuloUserDirCommand'))
        check.status_for_command("Execute command Initialize Accumulo on service Accumulo 1.6",
                                 service.service_command_by_name('AccumuloInitServiceCommand'))
        # check.status_for_command("Starting Accumulo Service", service.start())
