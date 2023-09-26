import unittest
from unittest.mock import MagicMock

from object_server_cluster import ObjectServerCluster


class TestObjectServerCluster(unittest.TestCase):

    def setUp(self):
        self.object_server_cluster = ObjectServerCluster()

    def test_replicate_object(self):
        object_key = "test_key"
        object_data = b"test_data"
        self.object_server_cluster.object_servers = [MagicMock(), MagicMock()]
        self.object_server_cluster.replicate_object(object_key, object_data)
        for object_server in self.object_server_cluster.object_servers:
            if object_server != self.object_server_cluster:
                object_server.storage_backend.write_object.assert_called_once_with(object_key, object_data)

    def test_delete_object(self):
        object_key = "test_key"
        self.object_server_cluster.object_servers = [MagicMock(), MagicMock()]
        self.object_server_cluster.delete_object(object_key)
        for object_server in self.object_server_cluster.object_servers:
            if object_server != self.object_server_cluster:
                object_server.storage_backend.delete_object.assert_called_once_with(object_key)

    def test_rebalance_objects(self):
        self.object_server_cluster.rebalance_objects_using_load_balancing = MagicMock()
        self.object_server_cluster.rebalance_interval = 1
        self.object_server_cluster.rebalance_objects()
        self.object_server_cluster.rebalance_objects_using_load_balancing.assert_called_once()
        self.assertEqual(self.object_server_cluster.rebalance_objects_using_load_balancing.call_count, 1)

    def test_exchange_heartbeats(self):
        self.object_server_cluster.send_heartbeat = MagicMock()
        self.object_server_cluster.heartbeat_interval = 1
        self.object_server_cluster.exchange_heartbeats()
        self.object_server_cluster.send_heartbeat.assert_called_once()
        self.assertEqual(self.object_server_cluster.send_heartbeat.call_count,
                         len(self.object_server_cluster.object_servers) - 1)

    def test_replicate_object_using_consensus(self):
        object_key = "test_key"
        object_data = b"test_data"
        self.object_server_cluster.object_servers = [MagicMock(), MagicMock()]
        self.object_server_cluster.redundancy_factor = 2
        self.object_server_cluster.consensus_threshold = 0.5
        self.object_server_cluster.replicate_object_using_consensus(object_key, object_data)
        for object_server in self.object_server_cluster.object_servers:
            if object_server != self.object_server_cluster:
                object_server.storage_backend.write_object.assert_called_once_with(object_key, object_data)

    def test_rebalance_objects_using_load_balancing(self):
        self.object_server_cluster.object_servers = [MagicMock(), MagicMock()]
        self.object_server_cluster.storage_backend.get_objects = MagicMock(return_value=[MagicMock(), MagicMock()])
        self.object_server_cluster.rebalance_objects_using_load_balancing()
        self.assertEqual(self.object_server_cluster.storage_backend.write_object.call_count, 1)

    def test_use_redundancy(self):
        self.object_server_cluster.object_servers = [MagicMock(), MagicMock()]
        self.object_server_cluster.storage_backend.objects = [MagicMock(), MagicMock()]
        self.object_server_cluster.redundancy_factor = 2
        self.object_server_cluster.use_redundancy()
        for object_server in self.object_server_cluster.object_servers:
            for obj in object_server.storage_backend.objects:
                object_server.storage_backend.write_object.assert_called_once_with(obj.key, obj.data)


if __name__ == '__main__':
    unittest.main()
