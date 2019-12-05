# To-Dos

## Current

- model messaging with changing object popularity
  - a thread to which a new message is posted becomes popular
  - indexing structure for full-text search
  - try to create a synthetic workload for this

## Long-term

- decouple scheduler from executor (which decides which queued transaction should be executed)
- we want objects of different types
- we can use similar weights (wait time and queue length) to those used in the queue-matching problem to decide what to schedule
- hierarchical locks (Legion project at Stanford)
