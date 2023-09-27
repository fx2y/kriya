from object_server import ObjectServerHandler, ObjectServer
from s3_protocol import S3Protocol

if __name__ == '__main__':
    # create object server
    server = ObjectServerHandler(('localhost', 8080), ObjectServer)

    # create S3 protocol handler
    s3_protocol = S3Protocol()

    # serve forever
    server.serve_forever()
