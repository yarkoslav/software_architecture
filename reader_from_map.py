import multiprocessing as mp
import hazelcast
from time import sleep 

key = "hello_hazelcast"

def without_blocking():
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("my_map").blocking()
    for _ in range(1000):
        value = my_map.get(key)
        sleep(1 / 1000)
        value += 1
        my_map.put(key, value)
    client.shutdown()

def with_pessimistic_lock():
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("my_map").blocking()
    for _ in range(1000):
        my_map.lock(key)
        try:
            value = my_map.get(key)
            sleep(1 / 1000)
            value += 1
            my_map.put(key, value)
        finally:
            my_map.unlock(key)
    client.shutdown()

def with_optimistic_lock():
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("my_map").blocking()
    for _ in range(1000):
        while True:
            value = my_map.get(key)
            new_value = value
            sleep(1 / 1000)
            new_value += 1
            if my_map.replace_if_same(key, value, new_value):
                break
    client.shutdown()

if __name__ == "__main__":
    methods = [without_blocking, with_pessimistic_lock, with_optimistic_lock]
    for method in methods:
        client = hazelcast.HazelcastClient()
        my_map = client.get_map("my_map").blocking()
        my_map.put(key, 0)
        processes = []
        for i in range(3):
            processes.append(mp.Process(target=method))
            processes[i].start()

        for i in range(3):
            processes[i].join()
        final_value = my_map.get(key)
        print(f"for method {method.__name__} results is : {final_value}")
        client.shutdown()
