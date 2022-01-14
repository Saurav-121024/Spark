import datetime
import json
import logging
import time
import traceback
from collections import namedtuple

from requests import Session
from requests.packages import urllib3

urllib3.disable_warnings()  # for self-signed certificates- still safe (just stops verbal warnings)


class BadRequest(Exception):
    pass


class AuthenticationError(Exception):
    pass


class NotFoundError(Exception):
    pass


class ServerError(Exception):
    pass


class OtherError(Exception):
    pass


class RetriesExhaustedError(Exception):
    pass


Response = namedtuple('Response', ['status_code', 'text', 'json', 'error', 'traceback'])


class IMSAPIConnector(object):
    def __init__(self, url, username, password, timeout=5, retries=72, backoff=5, insecure=False, logger=None):
        self._url = url.rstrip('/')
        self._username = username
        self._password = password
        self._timeout = timeout if timeout > 0 else 5
        self._retries = retries if retries > 0 else 72
        self._backoff = backoff if backoff > 0 else 5
        self._insecure = insecure
        self._logger = logger if logger is not None else logging.getLogger(self.__class__.__name__)

        self._session = None
        self._headers = None

    def __repr__(self):
        return '{0}({1})'.format(
            self.__class__.__name__,
            ', '.join(['{0}={1}'.format(k, repr(v)) for k, v in dict(
                url=self._url,
                username=self._username,
                password=self._password,
                logger=self._logger,
            ).items()])
        )

    def _build_url(self, resource):
        resource_parts = resource.split('?')
        resource_only = resource_parts[0]
        arguments_only = None
        if len(resource_parts) > 1:
            arguments_only = '?'.join(resource_parts[1:])

        return '{0}/{1}/{2}'.format(
            self._url,
            resource_only.strip('/'),
            '?{0}'.format(arguments_only) if arguments_only is not None else ''
        )

    def _do_request(self, method, kwargs, expect_json):
        method_name = method.upper()
        url = kwargs.get('url')

        self._logger.debug('sending {0} to {1}'.format(method_name, repr(url)))
        before = datetime.datetime.now()
        r = getattr(self._session, method)(**kwargs)
        after = datetime.datetime.now()
        self._logger.debug('returned in {0}'.format(after - before))

        if r.status_code not in [200, 201]:
            error_string = '{0} to {1} returned in {2}; response was {3} of {4}'.format(
                method_name,
                repr(url),
                after - before,
                r.status_code,
                repr(r.text)
            )

            self._logger.error('response was {0} of {1}'.format(
                r.status_code, repr(r.text)
            ))

            if r.status_code == 400:
                raise BadRequest(error_string)
            elif r.status_code in [401, 403]:
                raise AuthenticationError(error_string)
            elif r.status_code in [404]:
                raise NotFoundError(error_string)
            elif r.status_code == 500:
                raise ServerError(error_string)

        self._logger.debug('response was {0}'.format(repr(r.text)))

        json_data = None
        error = None
        traceback_data = None

        try:
            json_data = r.json()
        except BaseException as e:
            error = e
            traceback_data = traceback.format_exc()
            self._logger.error('failed to parse JSON because:\n{0}'.format(
                traceback_data
            ))

        return Response(
            status_code=r.status_code,
            text=r.text,
            json=json_data,
            error=error,
            traceback=traceback_data,
        )

    def connect(self):
        self._session = Session()
        self._headers = {
            'Content-Type': 'application/json',
            'Content-Encoding': 'gzip',
        }

        self._logger.debug('requesting key for {0}'.format(
            repr(self._username)
        ))

        response = self.post(
            resource='/auth/login/',
            data={
                'username': self._username,
                'password': self._password,
            },
        )

        self._logger.debug('reading key from response')

        key = response.json.get('key')
        if key is None:
            self._session = None
            self._headers = None

            message = 'failed to get "key" field from {0}'.format(
                response
            )

            self._logger.error(message)

            raise AuthenticationError(message)

        self._headers.update({
            'Authorization': 'Token {0}'.format(key)
        })

        self._logger.debug('key set to {0}'.format(repr(key)))

    def close(self):
        self._session = None
        self._headers = None

    def _do_connect_and_retries_as_required(self, method, kwargs):
        last_exception = None
        for i in range(1, self._retries + 1):
            if self._session is None:
                self.connect()

            try:
                return method(**kwargs)
            except AuthenticationError as e:
                self.close()
                last_exception = e
                self._logger.error(
                    'request {0} of {1} failed; sleeping for {2} seconds'.format(
                        i, self._retries, self._backoff
                    )
                )
                time.sleep(self._backoff)

        raise RetriesExhaustedError(
            'all retries exhaust; last exception was {0}({1})'.format(
                last_exception.__class__.__name__, repr(str(last_exception))
            )
        )

    def get(self, resource, data=None, expect_json=True, headers=None):
        return self._do_connect_and_retries_as_required(
            self._do_request,
            dict(
                method='get',
                kwargs=dict(
                    url=self._build_url(resource),
                    headers=headers,
                    params=data,
                    timeout=self._timeout,
                    verify=not self._insecure,
                ),
                expect_json=expect_json,
            )
        )

    def post(self, resource, data=None, expect_json=True, headers=None):
        return self._do_connect_and_retries_as_required(
            self._do_request,
            dict(
                method='post',
                kwargs=dict(
                    url=self._build_url(resource),
                    headers=headers if headers is not None else self._headers,
                    data=json.dumps(data) if data is not None else None,
                    timeout=self._timeout,
                    verify=not self._insecure,
                ),
                expect_json=expect_json,
            )
        )

    def put(self, resource, data=None, expect_json=True, headers=None):
        return self._do_connect_and_retries_as_required(
            self._do_request,
            dict(
                method='put',
                kwargs=dict(
                    url=self._build_url(resource),
                    headers=headers if headers is not None else self._headers,
                    data=json.dumps(data) if data is not None else None,
                    timeout=self._timeout,
                    verify=not self._insecure,
                ),
                expect_json=expect_json,
            )
        )

    def patch(self, resource, data=None, expect_json=True, headers=None):
        return self._do_connect_and_retries_as_required(
            self._do_request,
            dict(
                method='patch',
                kwargs=dict(
                    url=self._build_url(resource),
                    headers=headers if headers is not None else self._headers,
                    data=json.dumps(data) if data is not None else None,
                    timeout=self._timeout,
                    verify=not self._insecure,
                ),
                expect_json=expect_json,
            )
        )

    def delete(self, resource, data=None, expect_json=True, headers=None):
        return self._do_connect_and_retries_as_required(
            self._do_request,
            dict(
                method='delete',
                kwargs=dict(
                    url=self._build_url(resource),
                    headers=headers if headers is not None else self._headers,
                    data=json.dumps(data) if data is not None else None,
                    timeout=self._timeout,
                    verify=not self._insecure,
                ),
                expect_json=expect_json,
            )
        )

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
