import http.server
import os
import shutil
import socketserver
import urllib


class ObjectServer(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # handle GET request
        path = self.translate_path(self.path)
        f = None
        if os.path.isdir(path):
            # if path is a directory, serve index.html
            parts = urllib.parse.urlsplit(self.path)
            if not parts.path.endswith('/'):
                # redirect browser - doing basically what apache does
                self.send_response(301)
                new_parts = (parts[0], parts[1], parts[2] + '/', parts[3], parts[4])
                new_url = urllib.parse.urlunsplit(new_parts)
                self.send_header("Location", new_url)
                self.end_headers()
                return None
            path = os.path.join(path, 'index.html')
            if not os.path.exists(path):
                # if index.html does not exist, serve directory listing
                return self.list_directory(path)
        ctype = self.guess_type(path)
        try:
            # open the file and serve it
            f = open(path, 'rb')
        except IOError:
            self.send_error(404, "File not found")
            return None
        try:
            self.send_response(200)
            self.send_header("Content-type", ctype)
            fs = os.fstat(f.fileno())
            self.send_header("Content-Length", str(fs[6]))
            self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
            self.end_headers()
            return f
        except:
            f.close()
            raise

    def do_PUT(self):
        # parse request
        content_length = int(self.headers['Content-Length'])
        data = self.rfile.read(content_length)

        # write data to file
        with open(self.path, 'wb') as f:
            f.write(data)

        # send response
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        # handle POST request
        content_length = int(self.headers['Content-Length'])
        data = self.rfile.read(content_length)
        # process the data
        self.send_response(200)
        self.end_headers()

    def do_DELETE(self):
        # handle DELETE request
        path = self.translate_path(self.path)
        if os.path.exists(path):
            if os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
            self.send_response(204)
        else:
            self.send_error(404)


class ObjectServerHandler(socketserver.ThreadingMixIn, http.server.HTTPServer):
    def __init__(self, server_address, RequestHandlerClass):
        super().__init__(server_address, RequestHandlerClass)

    def handle_error(self, request, client_address):
        # handle errors
        import traceback
        import sys

        # get error message
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_message = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))

        # log error message
        print(f'Error: {error_message}')

    def verify_request(self, request, client_address):
        # verify request
        return True

    def process_request(self, request, client_address):
        self.finish_request(request, client_address)
        self.shutdown_request(request)


if __name__ == '__main__':
    server = ObjectServerHandler(('localhost', 8080), ObjectServer)
    server.serve_forever()
