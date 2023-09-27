import unittest

from storage_layer import StorageLayer


class TestStorageLayer(unittest.TestCase):
    def setUp(self):
        self.storage_layer = StorageLayer()

    def test_put_and_get_object(self):
        object_key = "test_key"
        object_data = "test_data"
        self.storage_layer.put_object(object_key, object_data)
        retrieved_data = self.storage_layer.get_object(object_key)
        self.assertEqual(retrieved_data, object_data)

    def test_get_nonexistent_object(self):
        object_key = "nonexistent_key"
        retrieved_data = self.storage_layer.get_object(object_key)
        self.assertIsNone(retrieved_data)


if __name__ == '__main__':
    unittest.main()
