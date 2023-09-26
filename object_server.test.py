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
        self.mock_storage_backend = MagicMock()
        self.server = HTTPServer(('localhost', 8080), ObjectServer)
        self.request = MockRequest('GET', '/test-object', {'X-Amz-Content-Sha256': 'test-sha256'})
        self.client_address = MockClientAddress('127.0.0.1', 12345)
        self.object_server = self.server.RequestHandlerClass(self.request, self.client_address, self.server)
        self.object_server.storage_backend = self.mock_storage_backend

    def test_do_GET_with_valid_request(self):
        # Arrange
        self.object_server.headers = {'X-Amz-Content-Sha256': 'valid-sha256'}
        self.object_server.path = '/test-object'
        self.mock_storage_backend.read_object.return_value = b'test-data'

        # Act
        self.object_server.do_GET()

        # Assert
        self.mock_storage_backend.read_object.assert_called_once_with('test-object')
        self.object_server.send_response.assert_called_once_with(200)
        self.object_server.send_header.assert_any_call('Content-Length', '9')
        self.object_server.end_headers.assert_called_once()
        self.object_server.wfile.write.assert_called_once_with(b'test-data')

    def test_do_GET_with_missing_header(self):
        # Arrange
        self.object_server.headers = {}
        self.object_server.path = '/test-object'

        # Act
        self.object_server.do_GET()

        # Assert
        self.object_server.send_error.assert_called_once_with(400, 'Bad Request',
                                                              'Missing required header: X-Amz-Content-Sha256')

    def test_do_PUT_with_valid_request(self):
        # Arrange
        self.object_server.headers = {'X-Amz-Content-Sha256': 'valid-sha256', 'Content-Length': '9'}
        self.object_server.path = '/test-object'
        self.object_server.rfile.read.return_value = b'test-data'

        # Act
        self.object_server.do_PUT()

        # Assert
        self.mock_storage_backend.write_object.assert_called_once_with('test-object', b'test-data')
        self.object_server.send_response.assert_called_once_with(200)
        self.object_server.end_headers.assert_called_once()

    def test_do_PUT_with_missing_header(self):
        # Arrange
        self.object_server.headers = {}
        self.object_server.path = '/test-object'

        # Act
        self.object_server.do_PUT()

        # Assert
        self.object_server.send_error.assert_called_once_with(400, 'Bad Request',
                                                              'Missing required header: X-Amz-Content-Sha256')

    def test_do_DELETE_with_valid_request(self):
        # Arrange
        self.object_server.headers = {'X-Amz-Content-Sha256': 'valid-sha256'}
        self.object_server.path = '/test-object'

        # Act
        self.object_server.do_DELETE()

        # Assert
        self.mock_storage_backend.delete_object.assert_called_once_with('test-object')
        self.object_server.send_response.assert_called_once_with(204)
        self.object_server.end_headers.assert_called_once()

    def test_do_DELETE_with_missing_header(self):
        # Arrange
        self.object_server.headers = {}
        self.object_server.path = '/test-object'

        # Act
        self.object_server.do_DELETE()

        # Assert
        self.object_server.send_error.assert_called_once_with(400, 'Bad Request',
                                                              'Missing required header: X-Amz-Content-Sha256')

    def test_do_HEAD_with_valid_request(self):
        # Arrange
        self.object_server.headers = {'X-Amz-Content-Sha256': 'valid-sha256'}
        self.object_server.path = '/test-object'
        self.mock_storage_backend.object_exists.return_value = True
        self.mock_storage_backend.get_object_size.return_value = 9

        # Act
        self.object_server.do_HEAD()

        # Assert
        self.mock_storage_backend.object_exists.assert_called_once_with('test-object')
        self.mock_storage_backend.get_object_size.assert_called_once_with('test-object')
        self.object_server.send_response.assert_called_once_with(200)
        self.object_server.send_header.assert_any_call('Content-Type', 'application/octet-stream')
        self.object_server.send_header.assert_any_call('Content-Length', '9')
        self.object_server.end_headers.assert_called_once()

    def test_do_HEAD_with_missing_header(self):
        # Arrange
        self.object_server.headers = {}
        self.object_server.path = '/test-object'

        # Act
        self.object_server.do_HEAD()

        # Assert
        self.object_server.send_error.assert_called_once_with(400, 'Bad Request',
                                                              'Missing required header: X-Amz-Content-Sha256')

    def test_do_POST_with_valid_request(self):
        # Arrange
        self.object_server.headers = {'X-Amz-Content-Sha256': 'valid-sha256', 'X-Amz-Access-Key': 'test-access-key',
                                      'X-Amz-Secret-Key': 'test-secret-key'}
        self.object_server.path = '/test-object'
        self.mock_storage_backend.create_object.return_value = True
        self.object_server.identity_layer.verify_access_key.return_value = True

        # Act
        self.object_server.do_POST()

        # Assert
        self.mock_storage_backend.create_object.assert_called_once_with('test-object')
        self.object_server.identity_layer.verify_access_key.assert_called_once_with('test-access-key',
                                                                                    'test-secret-key')
        self.object_server.send_response.assert_called_once_with(200)
        self.object_server.end_headers.assert_called_once()

    def test_do_POST_with_invalid_access_key(self):
        # Arrange
        self.object_server.headers = {'X-Amz-Content-Sha256': 'valid-sha256', 'X-Amz-Access-Key': 'test-access-key',
                                      'X-Amz-Secret-Key': 'test-secret-key'}
        self.object_server.path = '/test-object'
        self.mock_storage_backend.create_object.return_value = True
        self.object_server.identity_layer.verify_access_key.return_value = False

        # Act
        self.object_server.do_POST()

        # Assert
        self.object_server.identity_layer.verify_access_key.assert_called_once_with('test-access-key',
                                                                                    'test-secret-key')
        self.object_server.send_error.assert_called_once_with(403, 'Forbidden', 'Invalid access key or secret key.')
