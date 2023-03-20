import hazelcast
import multiprocessing as mp

def read_from_queue(rank, mutex):
    client = hazelcast.HazelcastClient()
    my_queue = client.get_queue("my_queue").blocking()

    while True:
        value = my_queue.take()
        mutex.acquire()
        print(f"Worker with {rank} rank read {value} from queue when size of queue was {my_queue.remaining_capacity()}")
        mutex.release()


if __name__ == "__main__":
    processes = []
    mutex = mp.Lock()
    for i in range(2):
        processes.append(mp.Process(target=read_from_queue, args=(i, mutex)))
        processes[i].start()
    