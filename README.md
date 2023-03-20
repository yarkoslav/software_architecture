# Lab 2: hello hazelcast

## Usage

To create cluster of 3 hazelcast nodes, please run:
```bash
docker compose up -d
```

To stop running our cluster:
```bash
docker compose down
```

To write 1000 values into the distributed map:
```bash
python writer_to_map.py
```

To show work of distributed map with different types of locks:
```bash
python reader_from_map.py
```

Write to the bounded queue:
```bash
python writer_to_queue.py
```

Read from bounded queue (with 2 processes):
```bash
python reader_from_queue.py
```