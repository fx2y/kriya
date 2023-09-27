import http.client
import unittest

from identity_layer import IdentityLayer
from storage_layer import StorageLayer


class TestObjectServer(unittest.TestCase):
    def setUp(self):
        self.server_address = ('localhost', 8000)
        self.identity_layer = IdentityLayer()
        self.storage_layer = StorageLayer()

    def test_put_object(self):
        # Create a test object.
        object_key = 'test-object'
        object_data = b'This is a test object.'

        # Put the object using the ObjectServer.
        conn = http.client.HTTPConnection(*self.server_address)
        headers = {
            'x-amz-access-key': 'test-access-key',
            'x-amz-secret-key': 'test-secret-key',
            'Content-Type': 'application/octet-stream',
            'Content-Length': str(len(object_data)),
        }
        conn.request('PUT', '/' + object_key, body=object_data, headers=headers)
        response = conn.getresponse()

        # Verify that the object was successfully stored.
        self.assertEqual(response.status, 200)
        self.assertEqual(self.storage_layer.get_object(object_key), object_data)

    def test_get_object(self):
        # Create a test object.
        object_key = 'test-object'
        object_data = b'This is a test object.'
        self.storage_layer.put_object(object_key, object_data)

        # Get the object using the ObjectServer.
        conn = http.client.HTTPConnection(*self.server_address)
        headers = {
            'x-amz-access-key': 'test-access-key',
            'x-amz-secret-key': 'test-secret-key',
        }
        conn.request('GET', '/' + object_key, headers=headers)
        response = conn.getresponse()

        # Verify that the object was successfully retrieved.
        self.assertEqual(response.status, 200)
        self.assertEqual(response.getheader('Content-Type'), 'application/octet-stream')
        self.assertEqual(response.getheader('Content-Length'), str(len(object_data)))
        self.assertEqual(response.read(), object_data)

    def test_invalid_credentials(self):
        # Try to access the server with invalid credentials.
        conn = http.client.HTTPConnection(*self.server_address)
        headers = {
            'x-amz-access-key': 'invalid-access-key',
            'x-amz-secret-key': 'invalid-secret-key',
        }
        conn.request('GET', '/', headers=headers)
        response = conn.getresponse()

        # Verify that the request was rejected.
        self.assertEqual(response.status, 401)

    def test_invalid_request(self):
        # Try to put an object with an invalid request.
        conn = http.client.HTTPConnection(*self.server_address)
        headers = {
            'x-amz-access-key': 'test-access-key',
            'x-amz-secret-key': 'test-secret-key',
            'Content-Type': 'application/octet-stream',
            'Content-Length': 'invalid-length',
        }
        conn.request('PUT', '/test-object', body=b'This is a test object.', headers=headers)
        response = conn.getresponse()

        # Verify that the request was rejected.
        self.assertEqual(response.status, 400)


if __name__ == '__main__':
    unittest.main()
