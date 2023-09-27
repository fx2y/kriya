import socketserver

from identity_layer import IdentityLayer
from object_server import ObjectServer
from storage_layer import StorageLayer

if __name__ == '__main__':
    # Initialize the identity layer.
    identity_layer = IdentityLayer()

    # Initialize the storage layer.
    storage_layer = StorageLayer()

    # Start the object server.
    handler = ObjectServer
    handler.identity_layer = identity_layer
    handler.storage_layer = storage_layer
    httpd = socketserver.TCPServer(("", 8080), handler)
    httpd.serve_forever()
