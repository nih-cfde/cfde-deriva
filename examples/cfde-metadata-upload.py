#!/usr/bin/env python

# This script uploads files using HTTP PUT to 
# https://examples.fair-research.org/public/CFDE/metadata/
#
# You can check the contents in the Globus UI
# https://app.globus.org/file-manager?origin_id=0e57d793-f1ac-4eeb-a30f-643b082d68ec&origin_path=%2Fpublic%2FCFDE%2Fmetadata%2F
#
# WARNING: This script will overwrite existing files
# 
# Usage:
# ./cfde-metadata-upload.py <file 1> ... <file N>
#
# Example:
# ./cfde-metadata-upload.py CFDE-all.ff95627.C2M2.bdbag.tgz
# PUT to https://317ec.36fe.dn.glob.us/public/CFDE/metadata/CFDE-all.ff95627.C2M2.bdbag.tgz status 200
#
# On first run on a new system, this script will initiate a Globus login.

import os.path
import sys
import requests
from fair_research_login import NativeClient

SCOPES = ['https://auth.globus.org/scopes/0e57d793-f1ac-4eeb-a30f-643b082d68ec/https',
              'urn:globus:auth:scope:transfer.api.globus.org:all']
CLIENT_ID = '728971e7-c1e5-4eba-bd06-af521d07938c'
APP_NAME = 'CFDE Metadata Upload'
ENDPOINT = '0e57d793-f1ac-4eeb-a30f-643b082d68ec'

# https://examples.fair-research.org/ points to https://317ec.36fe.dn.glob.us/
MY_URL_SPACE = 'https://317ec.36fe.dn.glob.us/public/CFDE/metadata/'

client = NativeClient(client_id=CLIENT_ID, app_name=APP_NAME)
client.login(requested_scopes=SCOPES, refresh_tokens=True)
tokens = client.load_tokens(requested_scopes=SCOPES)

https_token = tokens['0e57d793-f1ac-4eeb-a30f-643b082d68ec']['access_token']
headers = {'Authorization':'Bearer '+ https_token}

for file_to_put in sys.argv[1:]:
    file_name = os.path.basename(file_to_put)
    put_url = MY_URL_SPACE + file_name
    put_data = open(file_to_put, 'rb')  
    resp = requests.put(put_url,
                            headers=headers, data=put_data, allow_redirects=False)
    print('PUT to ' + put_url + ' status ' + str(resp.status_code))
