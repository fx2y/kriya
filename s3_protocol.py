import hashlib
import http
import sqlite3
import time


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

        # create object_servers table if it does not exist
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS object_servers
                                    (id INTEGER PRIMARY KEY, ip TEXT, port INTEGER, last_heartbeat INTEGER)''')
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

    def exchange_heartbeat(self, ip, port):
        # update last heartbeat for the object server
        self.cursor.execute('''UPDATE object_servers SET last_heartbeat = ? WHERE ip = ? AND port = ?''',
                            (int(time.time()), ip, port))
        self.conn.commit()

        # get all object servers except the current one
        self.cursor.execute('''SELECT ip, port FROM object_servers WHERE ip != ? AND port != ?''', (ip, port))
        object_servers = self.cursor.fetchall()

        # send heartbeat to all other object servers
        for object_server in object_servers:
            try:
                conn = http.client.HTTPConnection(object_server[0], object_server[1])
                conn.request('GET', '/heartbeat')
                response = conn.getresponse()
                if response.status != 200:
                    # handle error
                    pass
                conn.close()
            except:
                # handle error
                pass

    def replicate_objects(self, object_id, data):
        # get all object servers except the current one
        self.cursor.execute('''SELECT ip, port FROM object_servers WHERE id != ?''', (object_id,))
        object_servers = self.cursor.fetchall()

        # replicate object to all other object servers
        for object_server in object_servers:
            try:
                conn = http.client.HTTPConnection(object_server[0], object_server[1])
                conn.request('PUT', '/objects/' + str(object_id), data)
                response = conn.getresponse()
                if response.status != 200:
                    # handle error
                    pass
                conn.close()
            except:
                # handle error
                pass

    def rebalance_objects(self):
        # get all object servers and their object counts
        self.cursor.execute('''SELECT id, ip, port, COUNT(*) FROM object_servers
                            JOIN objects ON object_servers.id = objects.object_server_id
                            GROUP BY object_servers.id''')
        object_servers = self.cursor.fetchall()

        # calculate average object count per object server
        total_objects = sum([object_server[3] for object_server in object_servers])
        avg_objects = total_objects // len(object_servers)

        # rebalance objects among object servers
        for i in range(len(object_servers)):
            object_server = object_servers[i]
            if object_server[3] > avg_objects:
                # move objects to other object servers
                for j in range(i + 1, len(object_servers)):
                    other_object_server = object_servers[j]
                    if other_object_server[3] < avg_objects:
                        # move objects to other object server
                        self.cursor.execute('''UPDATE objects SET object_server_id = ? WHERE object_server_id = ?
                                            LIMIT ?''',
                                            (other_object_server[0], object_server[0], object_server[3] - avg_objects))
                        self.conn.commit()
                        break

    def add_object_server(self, ip, port):
        # add object server to the database
        self.cursor.execute('''INSERT INTO object_servers (ip, port, last_heartbeat) VALUES (?, ?, ?)''',
                            (ip, port, int(time.time())))
        self.conn.commit()

    def remove_object_server(self, ip, port):
        # remove object server from the database
        self.cursor.execute('''DELETE FROM object_servers WHERE ip = ? AND port = ?''', (ip, port))
        self.conn.commit()

    def get_object_servers(self):
        # get all object servers from the database
        self.cursor.execute('''SELECT ip, port FROM object_servers''')
        object_servers = self.cursor.fetchall()
        return object_servers
