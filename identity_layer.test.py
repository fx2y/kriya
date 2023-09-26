import os
import sqlite3
import tempfile
import unittest

from identity_layer import IdentityLayer


class TestIdentityLayer(unittest.TestCase):
    def setUp(self):
        self.db_fd, self.db_file = tempfile.mkstemp()
        self.identity_layer = IdentityLayer(self.db_file)
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('CREATE TABLE access_keys (access_key TEXT, secret_key TEXT)')
        conn.commit()
        conn.close()

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_file)

    def test_verify_access_key(self):
        # Test with valid access key and secret key
        access_key = 'access_key_1'
        secret_key = 'secret_key_1'
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('INSERT INTO access_keys (access_key, secret_key) VALUES (?, ?)', (access_key, secret_key))
        conn.commit()
        conn.close()
        self.assertTrue(self.identity_layer.verify_access_key(access_key, secret_key))

        # Test with invalid access key
        access_key = 'access_key_2'
        secret_key = 'secret_key_2'
        self.assertFalse(self.identity_layer.verify_access_key(access_key, secret_key))

        # Test with invalid secret key
        access_key = 'access_key_3'
        secret_key = 'secret_key_3'
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('INSERT INTO access_keys (access_key, secret_key) VALUES (?, ?)', (access_key, 'wrong_secret_key'))
        conn.commit()
        conn.close()
        self.assertFalse(self.identity_layer.verify_access_key(access_key, secret_key))


if __name__ == '__main__':
    unittest.main()
