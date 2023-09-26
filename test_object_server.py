import unittest
from http.server import HTTPServer
from io import BytesIO as IO
from unittest.mock import MagicMock

from object_server import ObjectServer


class MockRequest(IO):
    def __init__(self, method, path, headers):
        super().__init__(f"{method} {path} HTTP/1.1\r\n".encode())
        for key, value in headers.items():
            self.write(f"{key}: {value}\r\n".encode())
        self.write(b"\r\n")
        self.seek(0)
        self.is_closed = False

    def makefile(self, *args, **kwargs):
        return self

    def sendall(self, data):
        if not self.is_closed:
            self.write(data)

    def close(self):
        self.is_closed = True


class MockClientAddress(object):
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

    def __getitem__(self, index):
        if index == 0:
            return self.ip
        elif index == 1:
            return self.port
        else:
            raise IndexError("Client address index out of range")


class TestObjectServer(unittest.TestCase):
    def setUp(self):
        self.server = HTTPServer(('localhost', 8080), ObjectServer)
        self.request = MockRequest('GET', '/test-object', {'X-Amz-Content-Sha256': 'test-sha256'})
        self.client_address = MockClientAddress('127.0.0.1', 12345)
        self.object_server = self.server.RequestHandlerClass(self.request, self.client_address, self.server)
        self.object_server.storage_backend = MagicMock()

    def test_do_GET(self):
        # mock request
        self.object_server.path = '/test-object'
        self.object_server.headers = {'X-Amz-Content-Sha256': 'test-sha256'}
        self.object_server.storage_backend.read_object.return_value = b'test-data'

        # perform GET request
        self.object_server.do_GET()

        # assert response
        self.assertEqual(self.object_server.response_code, 200)
        self.assertEqual(self.object_server.headers['Content-Type'], 'application/octet-stream')
        self.assertEqual(self.object_server.headers['Content-Length'], str(len(b'test-data')))
        self.assertEqual(self.object_server.wfile.getvalue(), b'test-data')

    def test_do_PUT(self):
        # mock request
        self.object_server.path = '/test-object'
        self.object_server.headers = {'X-Amz-Content-Sha256': 'test-sha256'}
        self.object_server.rfile.read.return_value = b'test-data'

        # perform PUT request
        self.object_server.do_PUT()

        # assert response
        self.assertEqual(self.object_server.response_code, 200)
        self.object_server.storage_backend.write_object.assert_called_once_with('test-object', b'test-data')

    def test_do_DELETE(self):
        # mock request
        self.object_server.path = '/test-object'
        self.object_server.headers = {'X-Amz-Content-Sha256': 'test-sha256'}

        # perform DELETE request
        self.object_server.do_DELETE()

        # assert response
        self.assertEqual(self.object_server.response_code, 204)
        self.object_server.storage_backend.delete_object.assert_called_once_with('test-object')

    def test_do_HEAD(self):
        # mock request
        self.object_server.path = '/test-object'
        self.object_server.headers = {'X-Amz-Content-Sha256': 'test-sha256'}
        self.object_server.storage_backend.object_exists.return_value = True
        self.object_server.storage_backend.get_object_size.return_value = 10

        # perform HEAD request
        self.object_server.do_HEAD()

        # assert response
        self.assertEqual(self.object_server.response_code, 200)
        self.assertEqual(self.object_server.headers['Content-Type'], 'application/octet-stream')
        self.assertEqual(self.object_server.headers['Content-Length'], '10')
