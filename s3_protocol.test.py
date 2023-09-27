import unittest
from unittest.mock import MagicMock

from s3_protocol import S3Protocol


class TestS3Protocol(unittest.TestCase):
    def setUp(self):
        self.s3_protocol = S3Protocol()

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
        method, headers, body = self.s3_protocol.parse_request(request)
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
            self.s3_protocol.parse_request(request)

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
        self.s3_protocol.validate_headers(headers)

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
            self.s3_protocol.validate_headers(headers)

    def test_validate_body_valid(self):
        # Test valid body with allowed content type and valid MD5 checksum
        headers = {
            'Content-Type': 'application/octet-stream',
            'Content-MD5': '68b329da9893e34099c7d8ad5cb9c940'
        }
        body = b'hello world'
        self.s3_protocol.validate_body(headers, body)

    def test_validate_body_invalid_content_type(self):
        # Test invalid body with disallowed content type
        headers = {
            'Content-Type': 'text/plain',
            'Content-MD5': '68b329da9893e34099c7d8ad5cb9c940'
        }
        body = b'hello world'
        with self.assertRaises(Exception):
            self.s3_protocol.validate_body(headers, body)

    def test_validate_body_invalid_md5(self):
        # Test invalid body with incorrect MD5 checksum
        headers = {
            'Content-Type': 'application/octet-stream',
            'Content-MD5': '12345'
        }
        body = b'hello world'
        with self.assertRaises(Exception):
            self.s3_protocol.validate_body(headers, body)


if __name__ == '__main__':
    unittest.main()
