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

#! /usr/bin/env python

"""

This module implements Synnefo extractor for accounting information
 
comments:
 * extract data using the kamaki library to contact the Synnefo API
 * configured with /etc/snfssmConfig
 * outputs logging into the file configured as "logfile_path"
 * spool data to "spooldir_path" 
"""

import ConfigParser
import os
import sys
import logging
import copy

import snfssm
import utils_ssm

sys.path.append('/root/ssmAccounting')

from kamaki.clients.compute import ComputeClient
from kamaki.clients import astakos
from kamaki.clients import ClientError
from kamaki.cli import config as kamaki_config

# parse configuration file
conf = ConfigParser.ConfigParser()
conf.read( [ '/etc/snfssmConfig', os.path.expanduser('~/.snfssmConfig') ] )
config = {}
for item in ( 
    'astakos_url', 
    'compute_url',
    'logfile_path', 
    'debug_level', 
    'tenants', 
    'gocdb_sitename', 
    'cloud_type',
    'spooldir_path',
    'ssm_input_header', 
    'ssm_input_sep', 
    'ssm_input_path',
    'voms_tenants_mapping',
    'snf_occi_url'
    ):
    config[item] = conf.get( 'Main', item )

# setup logging
debugLevels = { 'INFO': logging.INFO, 'DEBUG': logging.DEBUG }
logging.basicConfig( filename=os.path.expanduser(config['logfile_path']), filemode='a', level=debugLevels[config['debug_level']], format="%(asctime)s %(levelname)s %(message)s", datefmt='%c')
logging.info('extraction of records started')
loggedconf = copy.copy(config)

nullValue = 'NULL'


# get spooled urs
extract = utils_ssm.get_spooled_urs(config)

# timestamp pass
lastrun = utils_ssm.last_run(config)
utils_ssm.timestamp_lastrun(config)

voms_mapping = utils_ssm.get_tenants_mapping(config)

# loop over configured tenants
tenants = config['tenants'].split(',')

try:
    astakos_url = config['astakos_url']
    configKamaki = kamaki_config.Config()
    token = configKamaki.get_cloud("default", "token")
except:
    logging.error("the authentication url or admin token is not valid")
    

for tenant in tenants: 

    user_token = token 
    computeClient = ComputeClient(config['compute_url'], user_token)
    
    astakosClient = astakos.AstakosClient(config['astakos_url'], user_token)
    
    user_details = astakosClient.authenticate()
    
    userID = user_details['access']['user']['id']
    tenantID = user_details['access']['user']['id']
              
    images = snfssm.get_images_ids(computeClient)

    tenant = 'EGI_FCTF'
    # get vo
    try:
        vo = voms_mapping[tenant]
        logging.debug('found VO <%s> mapped to tenant <%s>' % (vo, tenant))
    except:
        logging.debug('unable to find a VO for tenant <%s>' % tenant)
        vo = nullValue

    snfssm.compute_extract(computeClient,config, images, userID, vo, tenantID, extract)
  

# write usage records to spool file
utils_ssm.write_to_spool( extract, config )

spool = utils_ssm.get_spooled_urs( config )

# forward to SSM
utils_ssm.write_to_ssm( spool, config )
logging.info('records successfully wrote to SSM file <%s>' % config['ssm_input_path'])
utils_ssm.unspool_terminated_vms( spool )
utils_ssm.write_to_spool( spool, config )
