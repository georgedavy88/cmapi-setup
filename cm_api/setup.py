#!/usr/bin/env python
#Author - George Davy
from cmapi import *
from cmapi.initDeploy import parcel_action
from cmapi.hadoopServices import *
from cm_api.api_client import ApiResource

def main():
    parse_options()
    init_cluster()
    add_hosts_to_cluster()

    # Deploy CDH Parcel and GPL Extra Parcel skip if they are ACTIVATED
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    for cdh_parcel in initVar.cmx.parcel:
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
    if initVar.cmx.amon_password and initVar.cmx.rman_password:
        if initVar.manager.licensed():
            mgmt_roles.append('REPORTSMANAGER')
        initVar.manager(*mgmt_roles).setup()
        initVar.manager(*mgmt_roles).start()

    # Upload license
    if initVar.cmx.license_file:
        initVar.manager.upload_license()

    zookeeperSetup()
    hdfsSetup()
    hbaseSetup()
    yarnSetup()
    flumeSetup()
    sparkonyarnSetup()
    hiveSetup()
    impalaSetup()
    oozieSetup()
    hueSetup()

    initVar.cdh.restart_cluster()

if __name__ == "__main__":
    main()
