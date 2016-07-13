#!/usr/bin/env python
import re
import socket
import urllib2
import sys
import initVar
from cm_api.api_client import ApiResource
from cm_api.endpoints.hosts import *

def init_cluster():
    """
    Initialise Cluster
    :return:
    """
    print "> Initialise Cluster"
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    # Update Cloudera Manager configuration
    cm = api.get_cloudera_manager()

    def manifest_to_dict(manifest_json):
        if manifest_json:
            dir_list = json.load(
                urllib2.urlopen(manifest_json))['parcels'][0]['parcelName']
            parcel_part = re.match(r"^(.*?)-(.*)-(.*?)$", dir_list).groups()
            print "{'product': %s, 'version': %s}" % (str(parcel_part[0]).upper(), str(parcel_part[1]).lower())
            return {'product': str(parcel_part[0]).upper(), 'version': str(parcel_part[1]).lower()}
        else:
            raise Exception("Invalid manifest.json")

    # Install CDH5 latest version
    repo_url = ["%s/cdh5/parcels/%s" % (initVar.cmx.archive_url, initVar.cmx.cdh_version)]
    print "CDH5 Parcel URL: %s" % repo_url[0]
    initVar.cmx.parcel.append(manifest_to_dict(repo_url[0] + "/manifest.json"))

    # Install GPLEXTRAS5 to match CDH5 version
    repo_url.append('%s/gplextras5/parcels/%s' %
                    (initVar.cmx.archive_url, initVar.cmx.parcel[0]['version'].split('-')[0]))
    print "GPL Extras parcel URL: %s" % repo_url[1]
    initVar.cmx.parcel.append(manifest_to_dict(repo_url[1] + "/manifest.json"))

    cm.update_config({"REMOTE_PARCEL_REPO_URLS": "http://archive.cloudera.com/impala/parcels/latest/,"
                                                 "http://archive.cloudera.com/search/parcels/latest/,"
                                                 "http://archive.cloudera.com/spark/parcels/latest/,"
                                                 "http://archive.cloudera.com/sqoop-connectors/parcels/latest/,"
                                                 "http://archive.cloudera.com/accumulo-c5/parcels/latest,"
                                                 "%s" % ",".join([url for url in repo_url if url]),
                      "PHONE_HOME": False, "PARCEL_DISTRIBUTE_RATE_LIMIT_KBS_PER_SECOND": "102400"})

    if initVar.cmx.cluster_name in [x.name for x in api.get_all_clusters()]:
        print "Cluster name: '%s' already exists" % initVar.cmx.cluster_name
    else:
        print "Creating cluster name '%s'" % initVar.cmx.cluster_name
        api.create_cluster(name=initVar.cmx.cluster_name, version=initVar.cmx.cluster_version)


def add_hosts_to_cluster():
    """
    Add hosts to cluster
    :return:
    """
    print "> Add hosts to Cluster: %s" % initVar.cmx.cluster_name
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    cm = api.get_cloudera_manager()

    # deploy agents into host_list
    host_list = list(set([socket.getfqdn(x) for x in initVar.cmx.host_names] + [socket.getfqdn(initVar.cmx.cm_server)]) -
                     set([x.hostname for x in api.get_all_hosts()]))
    if host_list:
        cmd = cm.host_install(user_name=initVar.cmx.ssh_root_user, host_names=host_list,
                              password=initVar.cmx.ssh_root_password, private_key=initVar.cmx.ssh_private_key, unlimited_jce=True)

        # TODO: Temporary fix to flag for unlimited strength JCE policy files installation (If unset, defaults to false)
        # host_install_args = {"userName": initVar.cmx.ssh_root_user, "hostNames": host_list, "password": initVar.cmx.ssh_root_password,
        #                     "privateKey": initVar.cmx.ssh_private_key, "unlimitedJCE": True}
        # cmd = cm._cmd('hostInstall', data=host_install_args)
        print "Installing host(s) to cluster '%s' - [ http://%s:7180/cmf/command/%s/details ]" % \
              (socket.getfqdn(initVar.cmx.cm_server), initVar.cmx.cm_server, cmd.id)
        check.status_for_command("Hosts: %s " % host_list, cmd)

    hosts = []
    for host in api.get_all_hosts():
        if host.hostId not in [x.hostId for x in cluster.list_hosts()]:
            print "Adding {'ip': '%s', 'hostname': '%s', 'hostId': '%s'}" % (host.ipAddress, host.hostname, host.hostId)
            hosts.append(host.hostId)

    if hosts:
        print "Adding hostId(s) to '%s'" % initVar.cmx.cluster_name
        print "%s" % hosts
        cluster.add_hosts(hosts)


def host_rack():
    """
    Add host to rack
    :return:
    """
    # TODO: Add host to rack
    print "> Add host to rack"
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    hosts = []
    for h in api.get_all_hosts():
        # host = api.create_host(h.hostId, h.hostname,
        # socket.gethostbyname(h.hostname),
        # "/default_rack")
        h.set_rack_id("/default_rack")
        hosts.append(h)

    cluster.add_hosts(hosts)


def _check_parcel_stage(parcel_item, expected_stage, action_description):
    # def wait_for_parcel_stage(cluster, parcel, wanted_stages, action_description):
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)

    while True:
        cdh_parcel = cluster.get_parcel(product=parcel_item['product'], version=parcel_item['version'])
        if cdh_parcel.stage in expected_stage:
            break
        if cdh_parcel.state.errors:
            raise Exception(str(cdh_parcel.state.errors))

        msg = " [%s: %s / %s]" % (cdh_parcel.stage, cdh_parcel.state.progress, cdh_parcel.state.totalProgress)
        sys.stdout.write(msg + " " * (78 - len(msg)) + "\r")
        sys.stdout.flush()
        time.sleep(1)


def parcel_action(parcel_item, function, expected_stage, action_description):
    api = ApiResource(server_host=initVar.cmx.cm_server, username=initVar.cmx.username, password=initVar.cmx.password, version=initVar.cmx.api_version)
    cluster = api.get_cluster(initVar.cmx.cluster_name)
    print "%s [%s-%s]" % (action_description, parcel_item['product'], parcel_item['version'])
    cdh_parcel = cluster.get_parcel(product=parcel_item['product'], version=parcel_item['version'])

    cmd = getattr(cdh_parcel, function)()
    if not cmd.success:
        print "ERROR: %s failed!" % action_description
        exit(0)
    return _check_parcel_stage(parcel_item, expected_stage, action_description)
