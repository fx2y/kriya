import unittest

from identity_layer import IdentityLayer


class TestIdentityLayer(unittest.TestCase):
    def setUp(self):
        self.identity_layer = IdentityLayer()

    def test_verify_credentials_with_valid_keys(self):
        access_key = "valid_access_key"
        secret_key = "valid_secret_key"
        result = self.identity_layer.verify_credentials(access_key, secret_key)
        self.assertTrue(result)

    def test_verify_credentials_with_invalid_keys(self):
        access_key = "invalid_access_key"
        secret_key = "invalid_secret_key"
        result = self.identity_layer.verify_credentials(access_key, secret_key)
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
