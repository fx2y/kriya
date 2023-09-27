import http.server

from identity_layer import IdentityLayer
from storage_layer import StorageLayer


class ObjectServer(http.server.BaseHTTPRequestHandler):
    storage_layer = StorageLayer()
    identity_layer = IdentityLayer()

    def do_GET(self):
        # Extract the access key and secret key from the request headers.
        access_key = self.headers.get('x-amz-access-key')
        secret_key = self.headers.get('x-amz-secret-key')

        # Verify the access key and secret key using the identity layer.
        if not self.identity_layer.verify_credentials(access_key, secret_key):
            self.send_error(401, 'Unauthorized')
            return

        # Extract the HTTP method, headers, and body from the request.
        method = self.command
        headers = self.headers
        content_length = int(headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)

        # Validate the headers and body according to the S3 protocol.
        if not self.validate_request(headers, body):
            self.send_error(400, 'Invalid request')
            return

        # Perform the GET operation on the object.
        object_key = self.path[1:]
        object_data = self.storage_layer.get_object(object_key)

        # Return the object data to the client.
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Length', str(len(object_data)))
        self.end_headers()
        self.wfile.write(object_data)

    def do_PUT(self):
        # Extract the access key and secret key from the request headers.
        access_key = self.headers.get('x-amz-access-key')
        secret_key = self.headers.get('x-amz-secret-key')

        # Verify the access key and secret key using the identity layer.
        if not self.identity_layer.verify_credentials(access_key, secret_key):
            self.send_error(401, 'Unauthorized')
            return
        # Extract the HTTP method, headers, and body from the request.
        method = self.command
        headers = self.headers
        content_length = int(headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)

        # Validate the headers and body according to the S3 protocol.
        if not self.validate_request(headers, body):
            self.send_error(400, 'Invalid request')
            return

        # Perform the PUT operation on the object.
        object_key = self.path[1:]
        self.storage_layer.put_object(object_key, body)

        # Return a success response to the client.
        self.send_response(200)
        self.end_headers()

    def validate_request(self, headers, body):
        # Validate the headers and body according to the S3 protocol.
        # ...

        return True
