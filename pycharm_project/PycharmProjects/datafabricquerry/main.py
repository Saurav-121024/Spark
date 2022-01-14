import requests

# req_url = "http://kapi-oxygen-devteamcluster.dev.tsys.aws:8443/platform/imsdatafabric/intch/jxmoi001/fetch"
req_url = "http://kapi-oxygen-devteamcluster.dev.tsys.aws:8443/platform/imsdatafabric/fetch"
new_header = {
    'Content-Type' : "application/json",
    'Accept' : "application/json",
    'client-id' : '5701'
}
new_data = {
        "searchCriteria": {
            "rootSegmentKey": {
                "segmentName": "AMSAM00",
                "uniqueKeyValue": {
                    "CLIENT_NUM": "5701",
                    "APPLICATION_NUM": "000000000001",
                    "APPLICATION_SUFFIX": "00"
                }
            }
        },
        "retrieveSegmentFields": {
            "AMSAM00": ["*"]
        }
    }
s = requests.session()
response = s.post(req_url, headers=new_header, json=new_data)

print(response.json())

