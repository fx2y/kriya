import socket
import threading
import time


class ObjectServerCluster:
    """
    A cluster of object servers that work together to provide high availability and fault tolerance.

    Attributes:
    - object_servers: A list of object servers in the cluster.
    - heartbeat_interval: The interval (in seconds) at which object servers exchange heartbeats.
    - rebalance_interval: The interval (in seconds) at which objects are rebalanced across object servers.
    - consensus_threshold: The percentage of successful writes required for consensus.
    - redundancy_factor: The number of replicas to maintain for each object.
    - heartbeat_port: The port used for exchanging heartbeats.
    """

    def __init__(self):
        """
        Initializes a new instance of the ObjectServerCluster class.
        """
        self.object_servers = []
        self.heartbeat_interval = 5  # seconds
        self.rebalance_interval = 60  # seconds
        self.consensus_threshold = 0.5  # percentage
        self.redundancy_factor = 2  # number of replicas
        self.heartbeat_port = 5000

    def add_object_server(self, object_server):
        """
        Adds an object server to the cluster.

        Args:
        - object_server: The object server to add.
        """
        self.object_servers.append(object_server)

    def remove_object_server(self, object_server):
        """
        Removes an object server from the cluster.

        Args:
        - object_server: The object server to remove.
        """
        self.object_servers.remove(object_server)

    def replicate_object(self, object_key, object_data):
        """
        Replicates an object to all object servers in the cluster.

        Args:
        - object_key: The key of the object to replicate.
        - object_data: The data of the object to replicate.
        """
        for object_server in self.object_servers:
            if object_server != self:
                object_server.storage_backend.write_object(object_key, object_data)

    def delete_object(self, object_key):
        """
        Deletes an object from all object servers in the cluster.

        Args:
        - object_key: The key of the object to delete.
        """
        for object_server in self.object_servers:
            if object_server != self:
                object_server.storage_backend.delete_object(object_key)

    def rebalance_objects(self):
        """
        Rebalances objects across object servers at regular intervals.
        """
        while True:
            self.rebalance_objects_using_load_balancing()
            time.sleep(self.rebalance_interval)

    def exchange_heartbeats(self):
        """
        Exchanges heartbeats with all object servers in the cluster at regular intervals.
        """
        while True:
            for object_server in self.object_servers:
                if object_server != self:
                    object_server.send_heartbeat()
            time.sleep(self.heartbeat_interval)

    def send_heartbeat(self):
        """
        Sends a heartbeat message to all object servers in the cluster.
        """
        heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heartbeat_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        heartbeat_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        heartbeat_message = "heartbeat".encode()
        heartbeat_socket.sendto(heartbeat_message, ('<broadcast>', self.heartbeat_port))
        heartbeat_socket.close()

    def replicate_object_using_consensus(self, object_key: str, object_data: bytes):
        """
        Replicates an object to all object servers in the cluster using consensus.

        Args:
        - object_key: The key of the object to replicate.
        - object_data: The data of the object to replicate.
        """
        num_replicas = len(self.object_servers) * self.redundancy_factor
        num_successful_replicas = 0
        failed_replicas = []
        for object_server in self.object_servers:
            if object_server != self:
                if object_server.storage_backend.write_object(object_key, object_data):
                    num_successful_replicas += 1
                else:
                    failed_replicas.append(object_server)
        if num_successful_replicas / num_replicas < self.consensus_threshold:
            # Retry failed replicas
            for object_server in failed_replicas:
                if object_server.storage_backend.write_object(object_key, object_data):
                    num_successful_replicas += 1
            # If still below consensus threshold, remove failed object servers
            if num_successful_replicas / num_replicas < self.consensus_threshold:
                for object_server in failed_replicas:
                    self.remove_object_server(object_server)

    def rebalance_objects_using_load_balancing(self):
        """
        Rebalances objects across object servers using load balancing.
        """
        # Get the number of objects in each object server
        num_objects = []
        for object_server in self.object_servers:
            num_objects.append(len(object_server.storage_backend.objects))
        # Calculate the average number of objects per object server
        avg_num_objects = sum(num_objects) / len(num_objects)
        # Find the object server with the most objects
        max_num_objects = max(num_objects)
        max_object_server = self.object_servers[num_objects.index(max_num_objects)]
        # Find the object server with the least objects
        min_num_objects = min(num_objects)
        min_object_server = self.object_servers[num_objects.index(min_num_objects)]
        # If the difference between the max and min number of objects is greater than the average number of objects per object server
        if max_num_objects - min_num_objects > avg_num_objects:
            # Move objects from the max object server to the min object server
            num_objects_to_move = (max_num_objects - min_num_objects) // 2
            objects_to_move = max_object_server.storage_backend.get_objects(num_objects_to_move)
            for obj in objects_to_move:
                min_object_server.storage_backend.write_object(obj.key, obj.data)

    def use_redundancy(self):
        """
        Replicates all objects in the cluster to maintain redundancy.
        """
        for object_server in self.object_servers:
            for obj in object_server.storage_backend.objects:
                for i in range(self.redundancy_factor - 1):
                    object_server.storage_backend.write_object(obj.key, obj.data)

    def start(self):
        """
        Starts the cluster by starting the heartbeat and rebalance threads.
        """
        heartbeat_thread = threading.Thread(target=self.exchange_heartbeats)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        rebalance_thread = threading.Thread(target=self.rebalance_objects)
        rebalance_thread.daemon = True
        rebalance_thread.start()
