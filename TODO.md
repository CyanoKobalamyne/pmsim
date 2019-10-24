# To-Dos

## Current

- parallel scheduler: output n transactions per scheduling cycle
  - greedily in some order
- randomize transactions going into pool
- read papers: FPGA accelerated transactional memory, tightly integrated task scheduling (MICRO-52)

## Long-term

- queue multiple transactions from scheduler
- decouple scheduler from executor (which decides which queued transaction should be executed)
- we want objects of different types
- we can use similar weights (wait time and queue length) to those used in the queue-matching problem to decide what to schedule
