import hazelcast


if __name__ == "__main__":

    client = hazelcast.HazelcastClient()

    my_queue = client.get_queue("my_queue").blocking()
    my_queue.clear()

    for i in range(30):
        print(f"Adding element {i} into the queue, remaining space in queue: {my_queue.remaining_capacity()}")
        my_queue.put(i)
    client.shutdown()
