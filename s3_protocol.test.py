import unittest
from unittest.mock import MagicMock

from s3_protocol import S3Protocol


class TestS3Protocol(unittest.TestCase):
    def setUp(self):
        self.protocol = S3Protocol()

    def test_parse_request_valid(self):
        # Test a valid request
        request = MagicMock()
        request.method = 'PUT'
        request.headers = {
            'Authorization': 'Bearer token',
            'Content-Length': '10',
            'Content-MD5': '12345',
            'Content-Type': 'application/octet-stream',
            'Date': '2022-01-01',
            'Host': 'example.com',
            'x-amz-content-sha256': 'abcde',
            'x-amz-date': '2022-01-01T00:00:00Z'
        }
        request.rfile.read.return_value = b'0123456789'
        method, headers, body = self.protocol.parse_request(request)
        self.assertEqual(method, 'PUT')
        self.assertEqual(headers, request.headers)
        self.assertEqual(body, b'0123456789')

    def test_parse_request_invalid(self):
        # Test an invalid request with missing headers
        request = MagicMock()
        request.method = 'PUT'
        request.headers = {
            'Authorization': 'Bearer token',
            'Content-Length': '10',
            'Content-Type': 'application/octet-stream',
            'Date': '2022-01-01',
            'Host': 'example.com',
            'x-amz-content-sha256': 'abcde',
            'x-amz-date': '2022-01-01T00:00:00Z'
        }
        request.rfile.read.return_value = b'0123456789'
        with self.assertRaises(Exception):
            self.protocol.parse_request(request)

    def test_validate_headers_valid(self):
        # Test valid headers
        headers = {
            'Authorization': 'Bearer token',
            'Content-Length': '10',
            'Content-MD5': '12345',
            'Content-Type': 'application/octet-stream',
            'Date': '2022-01-01',
            'Host': 'example.com',
            'x-amz-content-sha256': 'abcde',
            'x-amz-date': '2022-01-01T00:00:00Z'
        }
        self.protocol.validate_headers(headers)

    def test_validate_headers_invalid(self):
        # Test invalid headers with missing required headers
        headers = {
            'Authorization': 'Bearer token',
            'Content-Length': '10',
            'Content-MD5': '12345',
            'Date': '2022-01-01',
            'Host': 'example.com',
            'x-amz-content-sha256': 'abcde',
            'x-amz-date': '2022-01-01T00:00:00Z'
        }
        with self.assertRaises(Exception):
            self.protocol.validate_headers(headers)

    def test_validate_body_valid(self):
        # Test valid body with allowed content type and valid MD5 checksum
        headers = {
            'Content-Type': 'application/octet-stream',
            'Content-MD5': '68b329da9893e34099c7d8ad5cb9c940'
        }
        body = b'hello world'
        self.protocol.validate_body(headers, body)

    def test_validate_body_invalid_content_type(self):
        # Test invalid body with disallowed content type
        headers = {
            'Content-Type': 'text/plain',
            'Content-MD5': '68b329da9893e34099c7d8ad5cb9c940'
        }
        body = b'hello world'
        with self.assertRaises(Exception):
            self.protocol.validate_body(headers, body)

    def test_validate_body_invalid_md5(self):
        # Test invalid body with incorrect MD5 checksum
        headers = {
            'Content-Type': 'application/octet-stream',
            'Content-MD5': '12345'
        }
        body = b'hello world'
        with self.assertRaises(Exception):
            self.protocol.validate_body(headers, body)

    def test_exchange_heartbeat(self):
        # Test that the last heartbeat for the object server is updated
        # and that a heartbeat is sent to all other object servers
        self.protocol.cursor.execute = MagicMock()
        self.protocol.conn.commit = MagicMock()
        self.protocol.http.client.HTTPConnection = MagicMock(return_value=MagicMock())
        self.protocol.http.client.HTTPConnection().request = MagicMock()
        self.protocol.http.client.HTTPConnection().getresponse = MagicMock(return_value=MagicMock(status=200))
        self.protocol.exchange_heartbeat('127.0.0.1', 8080)
        self.protocol.cursor.execute.assert_called_once_with(
            '''UPDATE object_servers SET last_heartbeat = ? WHERE ip = ? AND port = ?''',
            (MagicMock(), '127.0.0.1', 8080))
        self.protocol.conn.commit.assert_called_once()
        self.protocol.http.client.HTTPConnection.assert_called_once_with('127.0.0.2', 8081)
        self.protocol.http.client.HTTPConnection().request.assert_called_once_with('GET', '/heartbeat')
        self.protocol.http.client.HTTPConnection().getresponse.assert_called_once()

    def test_replicate_objects(self):
        # Test that an object is replicated to all other object servers
        self.protocol.cursor.execute = MagicMock()
        self.protocol.http.client.HTTPConnection = MagicMock(return_value=MagicMock())
        self.protocol.http.client.HTTPConnection().request = MagicMock()
        self.protocol.http.client.HTTPConnection().getresponse = MagicMock(return_value=MagicMock(status=200))
        self.protocol.replicate_objects(1, b'data')
        self.protocol.cursor.execute.assert_called_once_with('''SELECT ip, port FROM object_servers WHERE id != ?''',
                                                             (1,))
        self.protocol.http.client.HTTPConnection.assert_called_once_with('127.0.0.2', 8081)
        self.protocol.http.client.HTTPConnection().request.assert_called_once_with('PUT', '/objects/1', b'data')
        self.protocol.http.client.HTTPConnection().getresponse.assert_called_once()

    def test_rebalance_objects(self):
        # Test that objects are rebalanced among object servers
        self.protocol.cursor.execute = MagicMock()
        self.protocol.conn.commit = MagicMock()
        self.protocol.cursor.fetchone = MagicMock(return_value=('secret_key',))
        self.protocol.cursor.fetchall = MagicMock(return_value=[(1, '127.0.0.1', 8080, 10), (2, '127.0.0.2', 8081, 20)])
        self.protocol.rebalance_objects()
        self.protocol.cursor.execute.assert_called_once_with('''SELECT id, ip, port, COUNT(*) FROM object_servers
                            JOIN objects ON object_servers.id = objects.object_server_id
                            GROUP BY object_servers.id''')
        self.protocol.conn.commit.assert_called_once()
        self.protocol.cursor.execute.assert_called_with('''UPDATE objects SET object_server_id = ? WHERE object_server_id = ?
                                            LIMIT ?''',
                                                        (2, 1, 10 - 15))
        self.assertEqual(self.protocol.cursor.execute.call_count, 2)

    def test_add_object_server(self):
        # Test that an object server is added to the database
        self.protocol.cursor.execute = MagicMock()
        self.protocol.conn.commit = MagicMock()
        self.protocol.add_object_server('127.0.0.1', 8080)
        self.protocol.cursor.execute.assert_called_once_with(
            '''INSERT INTO object_servers (ip, port, last_heartbeat) VALUES (?, ?, ?)''',
            ('127.0.0.1', 8080, MagicMock()))
        self.protocol.conn.commit.assert_called_once()

    def test_remove_object_server(self):
        # Test that an object server is removed from the database
        self.protocol.cursor.execute = MagicMock()
        self.protocol.conn.commit = MagicMock()
        self.protocol.remove_object_server('127.0.0.1', 8080)
        self.protocol.cursor.execute.assert_called_once_with('''DELETE FROM object_servers WHERE ip = ? AND port = ?''',
                                                             ('127.0.0.1', 8080))
        self.protocol.conn.commit.assert_called_once()

    def test_get_object_servers(self):
        # Test that all object servers are returned from the database
        self.protocol.cursor.execute = MagicMock(return_value=[('127.0.0.1', 8080), ('127.0.0.2', 8081)])
        object_servers = self.protocol.get_object_servers()
        self.protocol.cursor.execute.assert_called_once_with('''SELECT ip, port FROM object_servers''')
        self.assertEqual(object_servers, [('127.0.0.1', 8080), ('127.0.0.2', 8081)])


if __name__ == '__main__':
    unittest.main()
