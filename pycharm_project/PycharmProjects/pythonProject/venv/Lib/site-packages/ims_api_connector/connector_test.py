import json
import unittest

from hamcrest import assert_that, equal_to
from mock import MagicMock, call, patch

from ims_api_connector import IMSAPIConnector, Response


def get_mock_response(data, text=None, status_code=200):
    response = MagicMock()
    response.json.return_value = data
    response.text = text if text is not None else json.dumps(data)
    response.status_code = status_code

    return response


class IMSAPIConnectorTest(unittest.TestCase):
    def setUp(self):
        self._subject = IMSAPIConnector(
            url='https://1.2.3.4:5678/api/',
            username='some_username',
            password='some_password',
        )

    def test_constructor(self):
        assert_that(
            self._subject._url,
            equal_to(
                'https://1.2.3.4:5678/api',
            )
        )

        assert_that(
            self._subject._username,
            equal_to('some_username')
        )

        assert_that(
            self._subject._password,
            equal_to('some_password')
        )

        assert_that(
            self._subject._timeout,
            equal_to(5)
        )

    def test_build_url(self):
        assert_that(
            self._subject._build_url('assets/?limit=4'),
            equal_to('https://1.2.3.4:5678/api/assets/?limit=4')
        )

    def test_get(self):
        self._subject._session = MagicMock()
        self._subject._session.get.return_value = get_mock_response([{'some': 'asset'}])
        self._subject._headers = {'some': 'headers'}

        assert_that(
            self._subject.get('assets/?limit=4'),
            equal_to(
                Response(status_code=200, text='[{"some": "asset"}]', json=[
                         {'some': 'asset'}], error=None, traceback=None)
            )
        )

    def test_post(self):
        self._subject._session = MagicMock()
        self._subject._session.post.return_value = get_mock_response([{'some': 'asset'}])
        self._subject._headers = {'some': 'headers'}

        assert_that(
            self._subject.post('assets/?limit=4'),
            equal_to(
                Response(status_code=200, text='[{"some": "asset"}]', json=[
                         {'some': 'asset'}], error=None, traceback=None)
            )
        )

    def test_put(self):
        self._subject._session = MagicMock()
        self._subject._session.put.return_value = get_mock_response([{'some': 'asset'}])
        self._subject._headers = {'some': 'headers'}

        assert_that(
            self._subject.put('assets/?limit=4'),
            equal_to(
                Response(status_code=200, text='[{"some": "asset"}]', json=[
                         {'some': 'asset'}], error=None, traceback=None)
            )
        )

    def test_patch(self):
        self._subject._session = MagicMock()
        self._subject._session.patch.return_value = get_mock_response([{'some': 'asset'}])
        self._subject._headers = {'some': 'headers'}

        assert_that(
            self._subject.patch('assets/?limit=4'),
            equal_to(
                Response(status_code=200, text='[{"some": "asset"}]', json=[
                         {'some': 'asset'}], error=None, traceback=None)
            )
        )

    def test_delete(self):
        self._subject._session = MagicMock()
        self._subject._session.delete.return_value = get_mock_response([{'some': 'asset'}])
        self._subject._headers = {'some': 'headers'}

        assert_that(
            self._subject.delete('assets/?limit=4'),
            equal_to(
                Response(status_code=200, text='[{"some": "asset"}]', json=[
                         {'some': 'asset'}], error=None, traceback=None)
            )
        )

    @patch('ims_api_connector.connector.Session')
    def test_connect(self, Session):
        session = MagicMock()
        session.post.return_value = get_mock_response({'key': 'some_key'})
        Session.return_value = session

        self._subject.connect()

        assert_that(
            session.mock_calls[0:1],
            equal_to([
                call.post(
                    data='{"username": "some_username", "password": "some_password"}',
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': 'Token some_key',
                        'Content-Encoding': 'gzip'},
                    timeout=5,
                    url='https://1.2.3.4:5678/api/auth/login/',
                    verify=True)
            ])
        )

        assert_that(
            self._subject._session,
            equal_to(session)
        )

        assert_that(
            self._subject._headers,
            equal_to({
                'Content-Type': 'application/json',
                'Authorization': 'Token some_key',
                'Content-Encoding': 'gzip'
            })
        )

    def test_close(self):
        self._subject._session = MagicMock()

        self._subject.close()

        assert_that(
            self._subject._session,
            equal_to(None)
        )
