import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

from identity_layer import IdentityLayer
from object_server_cluster import ObjectServerCluster


class ObjectServer(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_layer = IdentityLayer('kriya.db')
        self.object_server_cluster = ObjectServerCluster()

    def do_GET(self):
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # validate request according to S3 protocol
        if 'X-Amz-Content-Sha256' not in self.headers:
            self.send_error(400, 'Bad Request', 'Missing required header: X-Amz-Content-Sha256')
            return

        # extract object key from request
        object_key = parsed_url.path.lstrip('/')

        # perform read operation on object
        object_data = self.storage_backend.read_object(object_key)

        # return object data to client
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Length', str(len(object_data)))
        self.end_headers()
        self.wfile.write(object_data)

    def do_PUT(self):
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # validate request according to S3 protocol
        if 'X-Amz-Content-Sha256' not in self.headers:
            self.send_error(400, 'Bad Request', 'Missing required header: X-Amz-Content-Sha256')
            return

        # extract object key from request
        object_key = parsed_url.path.lstrip('/')

        # read object data from request body
        object_data = self.rfile.read(int(self.headers['Content-Length']))

        # perform write operation on object
        self.storage_backend.write_object(object_key, object_data)

        # return success response to client
        self.send_response(200)
        self.end_headers()

        # hash object data using SHA-256
        object_hash = hashlib.sha256(object_data).hexdigest()

        # store hash value as metadata of object
        self.storage_backend.write_metadata(object_key, 'hash', object_hash)

        # replicate object to other object servers
        self.object_server_cluster.replicate_object(object_key, object_data)

    def do_DELETE(self):
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # validate request according to S3 protocol
        if 'X-Amz-Content-Sha256' not in self.headers:
            self.send_error(400, 'Bad Request', 'Missing required header: X-Amz-Content-Sha256')
            return

        # extract object key from request
        object_key = parsed_url.path.lstrip('/')

        # perform delete operation on object
        self.storage_backend.delete_object(object_key)

        # return success response to client
        self.send_response(204)
        self.end_headers()

        # delete object from other object servers
        self.object_server_cluster.delete_object(object_key)

    def do_HEAD(self):
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # validate request according to S3 protocol
        if 'X-Amz-Content-Sha256' not in self.headers:
            self.send_error(400, 'Bad Request', 'Missing required header: X-Amz-Content-Sha256')
            return

        # extract object key from request
        object_key = parsed_url.path.lstrip('/')

        # check if object exists
        if not self.storage_backend.object_exists(object_key):
            self.send_error(404, 'Not Found', 'The specified key does not exist.')
            return

        # return success response to client
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Length', str(self.storage_backend.get_object_size(object_key)))
        self.end_headers()

    def do_POST(self):
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # validate request according to S3 protocol
        if 'X-Amz-Content-Sha256' not in self.headers:
            self.send_error(400, 'Bad Request', 'Missing required header: X-Amz-Content-Sha256')
            return

        # extract object key from request
        object_key = parsed_url.path.lstrip('/')

        # extract access key and secret key from request headers
        access_key = self.headers.get('X-Amz-Access-Key')
        secret_key = self.headers.get('X-Amz-Secret-Key')

        # verify access key and secret key using identity layer
        if not self.identity_layer.verify_access_key(access_key, secret_key):
            self.send_error(403, 'Forbidden', 'Invalid access key or secret key.')
            return

        # perform create operation on object
        self.storage_backend.create_object(object_key)

        # return success response to client
        self.send_response(200)
        self.end_headers()

        # rebalance objects among object servers
        self.object_server_cluster.rebalance_objects()


def main():
    # create object server instance
    object_server = HTTPServer(('localhost', 8080), ObjectServer)

    # start object server
    object_server.serve_forever()


if __name__ == '__main__':
    main()
