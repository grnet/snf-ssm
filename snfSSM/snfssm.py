# Copyright (C) 2014 GRNET S.A.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import logging
import datetime
from dateutil import parser
from dirq.QueueSimple import QueueSimple
import pytz
import pprint

# Kamaki libraries
from kamaki.clients.compute import ComputeClient
from kamaki.clients.cyclades import CycladesClient
from kamaki.clients import astakos
from kamaki.clients import ClientError
from kamaki.cli import config as kamaki_config

snf_vm_statuses = {
    'ACTIVE':            'started',
    'BUILD':             'started',
    'REBOOT':            'started',
    'DELETED' :          'completed',
    'STOPPED':           'stopped',
    'ERROR':             'stopped',
    'UNKNOWN':           'unknown',
}
nullValue = 'NULL'


# returns a dict of available images for a specific user
def get_images_ids(computeClient):
    images = {}
    
    retrieved_images = computeClient.list_images()

    for image in retrieved_images:
        logging.debug("found image <name=%s, id=%s>" % ( image['name'], image['id']) )
        images[image['id']] = image['name']                

    logging.debug( "available images: %s" + str(images) )
    return images


# Creates the record for each VM for a specific userID
def compute_extract(compClient ,config, images, userID, vo, tenantID, spool):

    # spool new URs (those related to new VMs)
    logging.debug('extracting data for new VMs')
    
    servers = compClient.list_servers()

    for serverID in spool:
        serverID = serverID.replace(config['snf_occi_url'],'').strip()
        if serverID not in servers:
            servers.append({'id':serverID})
       
    for server in servers:
        
        details = compClient.get_server_details(server['id'])
        
        if not spool.has_key(server['id']):

            logging.debug('adding new record to spool for instance id <%s>' % server['id'])
            
            spool[server['id']] = {
                'VMUUID':             config['snf_occi_url'] + str(server['id']),
                'SiteName':           config['gocdb_sitename'],
                'MachineName':        server['name'], 
                'LocalUserId':        details['user_id'],
                'LocalGroupId':       details['tenant_id'],
                'FQAN':               nullValue,
                'Status':             nullValue,
                'StartTime':          nullValue,
                'EndTime':            nullValue, 
                'SuspendDuration':    nullValue,
                'WallDuration':       nullValue,
                'CpuDuration':        nullValue,
                'CpuCount':           nullValue,
                'NetworkType':        nullValue,
                'NetworkInbound':     '0',
                'NetworkOutbound':    '0',
                'Memory':             nullValue,
                'Disk':               nullValue,
                'StorageRecordId':    nullValue,
                'ImageId':            nullValue,
                'CloudType':          config['cloud_type'],
                'VO':                 vo,
                'VOGroup':            nullValue,
                'VORole':             nullValue,
                }

            try:
                logging.debug('trying to find out image id (depends on afterward deletion)')
                
                imid = str(details['image']['id'])
                spool[server['id']]['ImageId'] = images[imid]
            except:
                logging.debug( "image id=%s not available in glance anymore" % imid )
                spool[server['id']]['ImageId'] = imid
                
        else:
            logging.debug('VM <%s> status has changed' % server['id'])
        
       
        now = datetime.datetime.now(pytz.UTC)
        started = parser.parse(details['created'])
        updated = parser.parse(details['updated'])
        delta = now - started
        spool[server['id']]['StartTime']     = started.strftime("%s")
        spool[server['id']]['WallDuration']  = delta.seconds + delta.days * 24 * 3600
        spool[server['id']]['CpuDuration'] = spool[server['id']]['WallDuration']
	flavor = compClient.get_flavor_details(details['flavor']['id'])
        spool[server['id']]['CpuCount']      = flavor['vcpus']
        spool[server['id']]['Memory']        = flavor['ram']
        spool[server['id']]['Disk']          = flavor['disk']
       
        try:
            spool[server['id']]['Status'] = snf_vm_statuses[details['status']]
        except:
            logging.error( "unknown state <%s>" % details['status'] )
            spool[server['id']]['Status'] = 'unknown'
        
        if details['updated'] != None:
            if spool[server['id']]['Status']=='stopped':
                ended = parser.parse( details['updated'] )
                spool[server['id']]['EndTime']   = ended.strftime("%s")
                spool[server['id']]['SuspendDuration']=delta.seconds + delta.days * 24 * 3600
            else:
                ended = now
                spool[server['id']]['EndTime']   = ended.strftime("%s")
                spool[server['id']]['SuspendDuration']=0
        
        #print "SPOOL USAGE RECORDS"
        #print spool
    




       



