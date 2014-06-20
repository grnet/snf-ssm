"""
author: mpuel@in2p3.fr
date: lundi 23 avril 2012, 13:17:15 (UTC+0200)
copyright: Copyright (c) by IN2P3 computing centre, Villeurbanne (Lyon), France


"""
# The following functions for handling extracted accounting records with APEL/SSM 
# are as in https://github.com/EGI-FCTF/osssm/blob/master/SOURCES/usr/share/pyshared/osssm.py


import json
import logging
import time
import os.path
from dirq.QueueSimple import QueueSimple

nullValue = 'NULL'
orderedFields = [ 'VMUUID', 'SiteName', 'MachineName', 'LocalUserId', 'LocalGroupId', 'FQAN', 'Status', 'StartTime', 'EndTime', 'SuspendDuration', 'WallDuration', 'CpuDuration', 'CpuCount', 'NetworkType', 'NetworkInbound', 'NetworkOutbound', 'Memory', 'Disk', 'StorageRecordId', 'ImageId', 'CloudType' ]
stu_date_format = '%Y-%m-%d %H:%M:%S.0'

def write_to_ssm( extract, config ):
    """forwards usage records to SSM"""

    # ensure outgoing directory existence
    ssm_input_path = os.path.expanduser(config['ssm_input_path'])
    if not os.access(ssm_input_path, os.F_OK):
        os.makedirs(ssm_input_path, 0755)
    
    # only write non void URs file
    if len(extract) > 0:
        output = config['ssm_input_header'] + "\n"

        # itterate over VMs
        for vmname in extract.keys():
            logging.debug("generating ssm input file for VM %s" % vmname)
            for item in orderedFields:
                logging.debug("generating record %s: %s" % (item, extract[vmname][item]) )
                output += "%s: %s\n" % ( item, extract[vmname][item] )
            output += config['ssm_input_sep'] + "\n"

        # write file
        try:
            dirq = QueueSimple(ssm_input_path)
            dirq.add(output)
        except:
            logging.error('unable to push message in apel-ssm queue <%s>' % ssm_input_path)
    else:
        logging.debug('no usage records, skip forwarding to SSM')


def write_to_spool(extract, config ):
    """write extracted usage records to the spool file"""

    # move URs to json format
    data = json.dumps(extract)
    logging.debug("dumping extract to json format: <%s>" % data)

    # write to file    
    outfile = os.path.expanduser(config['spooldir_path'] + '/servers')
    try:        
        f = open( outfile, 'w' )
        f.write(data)
        f.close()
    except:
        logging.error("an error occured while dumping usage records to spool file <%s>" % outfile)
    logging.debug("usage records successfully dumped to spool file <%s>" % outfile)



def get_spooled_urs( config ):
    """read spooled usage records"""

    spooled_ur = None
    try:
        infile = os.path.expanduser(config['spooldir_path'] + '/servers')
        f = open( infile, 'r' )
        data = f.read()
        f.close()
        spooled_ur = json.loads(data)
        logging.debug("spooled URs have been read successfully: %s" % spooled_ur)
    except:
        logging.error("an error occured while reading the spool file <%s>" % infile)
    if spooled_ur == None:
        spooled_ur = {}
    
    return spooled_ur


def merge_records( new_urs, config ):
    """merge the extracted records with the ones spooled"""
    
    spooled_urs = get_spooled_urs( config )

    # update existing urs
    spooled_urs.update( new_urs )
    
    return spooled_urs


def timestamp_lastrun( config ):
    """touch timestamp in the spool directory"""

    timestamp = os.path.expanduser(config['spooldir_path'] + '/timestamp')
    try:
        open(timestamp, "w").close()
        logging.debug("touched timestamp <%s>" % timestamp)
    except:
        logging.error("unable to touch timestamp file <%s>" % timestamp)


def last_run( config ):
    """returns timestamp of the last extract pass, now if non-existent (stu_date_format)"""

    timestamp = os.path.expanduser(config['spooldir_path'] + '/timestamp')

    if os.path.exists(timestamp):        
        date = os.path.getmtime(timestamp)
    else:
        logging.debug("no timestamped loged run, return *now*")
        date = None

    lastrun = time.strftime( stu_date_format, time.gmtime(date) )
    logging.debug("last loged run at <%s>" % lastrun)
    return lastrun


def oldest_vm_start( config, spooled_servers, lastrun ):
    """returns the oldest accounted VM creation time (stu_date_format)"""

    # get creation dates for spooled VMs records
    spooled_creations = []
    for vm in spooled_servers.keys():
        start = spooled_servers[vm]['StartTime']
        if start != nullValue:
            spooled_creations += [ int(start) ]
    spooled_creations.sort()

    # if no spooled VM, test current VMs
    if spooled_creations == None or len(spooled_creations) == 0:
        logging.debug("no spooled VMs, return last run")
        oldest = lastrun
    else:
        oldest = time.strftime( stu_date_format, time.gmtime(spooled_creations[0]) )
    logging.debug("oldest_vm_start -> %s" % oldest)
    return oldest
        


def unspool_terminated_vms( spool ):
    """remove terminated VMs from the spool. This should occur only when VM usage record
       has been successfully forwarded to SSM"""

    for vmid in spool.keys():
        if spool[vmid]['Status'] == 'completed' or spool[vmid]['Status'] == 'error':
            del spool[vmid]

def get_tenants_mapping( config ): 
    """read spooled usage records"""

    voms_json = None
    voms_tenants = {}
    try:
        infile = os.path.expanduser(config['voms_tenants_mapping'])
        f = open( infile, 'r' )
        data = f.read()
        f.close()
        voms_json = json.loads(data)
        logging.debug("voms tenants mappings have been read successfully: %s" % voms_json)
    except:
        logging.error("an error occured while reading the voms tenants mapping file <%s>" % infile)
    if voms_json == None:
        voms_json = {}

    for vo in voms_json.keys():
        try:
            voms_tenants[voms_json[vo]['tenant']] = vo
            logging.debug('VO <%s> is mapped to tenant <%s>' % (vo, voms_json[vo]['tenant']))
        except:
            pass

    return voms_tenants