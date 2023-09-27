import os
import threading
import unittest
import urllib.request

from object_server import ObjectServerHandler, ObjectServer


class TestObjectServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Start the object server in a separate thread
        cls.server = ObjectServerHandler(('localhost', 8080), ObjectServer)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        # Stop the object server
        cls.server.shutdown()
        cls.server_thread.join()

    def test_get_existing_file(self):
        # Test GET request for an existing file
        url = 'http://localhost:8080/index.html'
        response = urllib.request.urlopen(url)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.headers['Content-Type'], 'text/html')
        self.assertEqual(response.read(), b'<html><body><h1>Hello, World!</h1></body></html>')

    def test_get_nonexistent_file(self):
        # Test GET request for a nonexistent file
        url = 'http://localhost:8080/nonexistent.html'
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(url)
        self.assertEqual(cm.exception.code, 404)

    def test_put_file(self):
        # Test PUT request to create a new file
        url = 'http://localhost:8080/newfile.txt'
        data = b'This is a new file.'
        req = urllib.request.Request(url, data=data, method='PUT')
        with urllib.request.urlopen(req) as response:
            self.assertEqual(response.status, 200)
        self.assertTrue(os.path.exists('/Users/abdullah/Documents/projects/kriya/newfile.txt'))
        with open('/Users/abdullah/Documents/projects/kriya/newfile.txt', 'rb') as f:
            self.assertEqual(f.read(), data)

    def test_post_request(self):
        # Test POST request
        url = 'http://localhost:8080/'
        data = b'This is a POST request.'
        req = urllib.request.Request(url, data=data, method='POST')
        with urllib.request.urlopen(req) as response:
            self.assertEqual(response.status, 200)

    def test_delete_file(self):
        # Test DELETE request to delete an existing file
        url = 'http://localhost:8080/index.html'
        req = urllib.request.Request(url, method='DELETE')
        with urllib.request.urlopen(req) as response:
            self.assertEqual(response.status, 204)
        self.assertFalse(os.path.exists('/Users/abdullah/Documents/projects/kriya/index.html'))


if __name__ == '__main__':
    unittest.main()
