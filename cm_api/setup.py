#!/usr/bin/env python
#Author - George Davy
import cmapi import *
from cm_api.api_client import ApiResource

def main():
    parse_options()
    init_cluster()
    add_hosts_to_cluster()

    # Deploy CDH Parcel and GPL Extra Parcel skip if they are ACTIVATED
    api = ApiResource(server_host=api.cm_server, username=api.username, password=api.password, version=api.api_version)
    cluster = api.get_cluster(api.cluster_name)
    for cdh_parcel in api.parcel:
        print "> Parcel action for parcel: [ %s-%s ]" % (cdh_parcel['product'], cdh_parcel['version'])
        parcel = cluster.get_parcel(product=cdh_parcel['product'], version=cdh_parcel['version'])
        if "ACTIVATED" not in parcel.stage:
            parcel_action(parcel_item=cdh_parcel, function="start_removal_of_distribution",
                          expected_stage=['DOWNLOADED', 'AVAILABLE_REMOTELY', 'ACTIVATING'],
                          action_description="Un-Distribute Parcel")
            parcel_action(parcel_item=cdh_parcel, function="start_download",
                          expected_stage=['DOWNLOADED'], action_description="Download Parcel")
            parcel_action(parcel_item=cdh_parcel, function="start_distribution", expected_stage=['DISTRIBUTED'],
                          action_description="Distribute Parcel")
            parcel_action(parcel_item=cdh_parcel, function="activate", expected_stage=['ACTIVATED'],
                          action_description="Activate Parcel")

    # Skip MGMT role installation if amon_password and rman_password password are False
    mgmt_roles = ['SERVICEMONITOR', 'ALERTPUBLISHER', 'EVENTSERVER', 'HOSTMONITOR']
    if api.amon_password and api.rman_password:
        if manager.licensed():
            mgmt_roles.append('REPORTSMANAGER')
        manager(*mgmt_roles).setup()
        manager(*mgmt_roles).start()

    # Upload license
    if api.license_file:
        manager.upload_license()

    setup_zookeeper()
    setup_hdfs()
    setup_hbase()
    setup_yarn()
    setup_flume()
    setup_spark_on_yarn()
    setup_hive()
    setup_impala()
    setup_oozie()
    setup_hue()

    cdh.restart_cluster()

if __name__ == "__main__":
    main()
