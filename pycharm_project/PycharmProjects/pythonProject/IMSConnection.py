from ims_api_connector import IMSAPIConnector

import jaydebeapi

conn = jaydebeapi.connect('com.ibm.ims.jdbc.IMSDriver', ['jdbc:ims://tn3270.tedc.totalsystem.net:1023', 'GP5KUM', 'DUB@2022'], )