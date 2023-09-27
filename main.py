import socketserver

from object_server import ObjectServer
from storage_layer import StorageLayer

if __name__ == '__main__':
    # Initialize the storage layer.
    storage_layer = StorageLayer()

    # Start the object server.
    handler = ObjectServer
    handler.storage_layer = storage_layer
    httpd = socketserver.TCPServer(("", 8080), handler)
    httpd.serve_forever()
