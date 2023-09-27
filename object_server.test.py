import http.client
import threading
import time
import unittest

from object_server import ObjectServer


class TestObjectServer(unittest.TestCase):
    def setUp(self):
        # Start the object server in a separate thread.
        self.server = ObjectServer(('localhost', 8000))
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

        # Wait for the server to start up.
        time.sleep(0.1)

    def tearDown(self):
        # Shut down the object server.
        self.server.shutdown()
        self.thread.join()

    def test_put_and_get_object(self):
        # Put an object to the server.
        conn = http.client.HTTPConnection('localhost', 8000)
        conn.request('PUT', '/test-object', b'This is a test object')
        response = conn.getresponse()
        self.assertEqual(response.status, 200)

        # Get the object from the server.
        conn = http.client.HTTPConnection('localhost', 8000)
        conn.request('GET', '/test-object')
        response = conn.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.read(), b'This is a test object')

    def test_get_nonexistent_object(self):
        # Try to get a nonexistent object from the server.
        conn = http.client.HTTPConnection('localhost', 8000)
        conn.request('GET', '/nonexistent-object')
        response = conn.getresponse()
        self.assertEqual(response.status, 404)

    def test_put_invalid_request(self):
        # Try to put an object with an invalid request.
        conn = http.client.HTTPConnection('localhost', 8000)
        conn.request('PUT', '/test-object', b'This is a test object', headers={'Content-Length': 'invalid'})
        response = conn.getresponse()
        self.assertEqual(response.status, 400)

    def test_get_invalid_request(self):
        # Try to get an object with an invalid request.
        conn = http.client.HTTPConnection('localhost', 8000)
        conn.request('GET', '/test-object', headers={'Content-Length': 'invalid'})
        response = conn.getresponse()
        self.assertEqual(response.status, 400)


if __name__ == '__main__':
    unittest.main()
