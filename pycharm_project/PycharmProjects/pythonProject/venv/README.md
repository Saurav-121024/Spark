# ims-python-api-connector

Connector for the FTP Technologies IMS HTTP API

## How to install

`$ pip install ims-api-connector`

## How to use

There are `.connect()` and `.close()` methods exposed as well as a context manager API (`__enter__()` and `__exit__()`) but you don't have to use either- simply calling `.get()`, `.post()` etc will handle the authentication for you.

Additionally, it will retry authentication if it picks up an authentication error during a request (to handle expired tokens etc) but if the attempt to re-authenticate fails, it will stop retrying.

```
from ims_api_connector import IMSAPIConnector

connector = IMSAPIConnector(
    url='https://some.ims.instance.com/api/',
    username='some_username',
    password='some_password',
    timeout=5,  # optional; seconds until a request times out
    retries=6,  # optional; number of re-auth attempts during a request
    backoff=5,  # optional; seconds between re-auth attempts
    insecure=False,  # optional; ignore certificate warnings
    logger=None,  # optional; Python logger object
)

connector.get('assets/?limit=50000')
```

Which returns an object like this:

```
Response(
    status_code=200,
    text='{"some": "data"}',
    json={'some': 'data'},
    error=None,  # any JSON parse exception object
    traceback=None,  # traceback string for JSON parse exception
)
```

## How to develop

Prerequisites:

* pip
* virtualenvwrapper
* entr

Instructions:

* Create a virtualenv
    * `mkvirtualenv ims-python-api-connector`
* Install the requirements
    * `pip install -r requirements.txt`
* Watch the tests
    * ./watch_tests.sh
* Write some code
