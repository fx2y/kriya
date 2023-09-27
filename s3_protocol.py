import hashlib
import sqlite3


class S3Protocol:
    def __init__(self):
        # create a connection to the SQLite database
        self.conn = sqlite3.connect('kriya.db')
        self.cursor = self.conn.cursor()

        # create the access_keys table if it doesn't exist
        self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS access_keys (
                        access_key TEXT PRIMARY KEY,
                        secret_key TEXT
                    )
                ''')
        self.conn.commit()

        self.allowed_content_types = ['application/octet-stream', 'binary/octet-stream',
                                      'application/x-www-form-urlencoded', 'multipart/form-data']

    def parse_request(self, request):
        # parse HTTP request from clients and validate them according to the S3 protocol
        method, headers, body = self.extract_request(request)
        self.validate_headers(headers)
        self.validate_body(headers, body)
        return method, headers, body

    def extract_request(self, request):
        method, headers, body = None, None, None
        try:
            method = request.method
            headers = dict(request.headers)
            body = request.rfile.read(int(headers.get('Content-Length', 0)))
        except Exception as e:
            raise Exception(f"Failed to extract request: {e}")
        return method, headers, body

    def validate_headers(self, headers):
        # validate headers according to the S3 protocol
        required_headers = ['Authorization', 'Content-Length', 'Content-MD5', 'Content-Type', 'Date', 'Host',
                            'x-amz-content-sha256', 'x-amz-date']
        for header in required_headers:
            if header not in headers:
                raise Exception(f"Missing required header: {header}")

    def validate_body(self, headers, body):
        # validate body according to the S3 protocol
        content_type = headers.get('Content-Type', '')
        if content_type not in self.allowed_content_types:
            raise Exception(f"Invalid content type: {content_type}")
        if 'Content-MD5' in headers:
            md5 = hashlib.md5(body).hexdigest()
            if md5 != headers['Content-MD5']:
                raise Exception("Invalid MD5 checksum")

    def authenticate_request(self, headers):
        # extract the access key and secret key from the request headers
        access_key = headers.get('access_key')
        secret_key = headers.get('secret_key')

        # verify the access key and secret key using the identity layer
        if not self.verify_identity(access_key, secret_key):
            # return an error response if the access key or secret key is invalid
            return False

        return True

    def verify_identity(self, access_key, secret_key):
        # verify the access key and secret key using the identity layer
        self.cursor.execute('SELECT secret_key FROM access_keys WHERE access_key = ?', (access_key,))
        result = self.cursor.fetchone()
        if result is not None and result[0] == secret_key:
            return True
        else:
            return False
