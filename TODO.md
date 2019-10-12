# To-Dos

## Current

- switch memory size with core number as parameter
- choose transaction from fixed-size pool
- parallel scheduler: output n transactions per scheduling cycle

## Long-term

- queue multiple transactions from scheduler
- decouple scheduler from executor (which decides which queued transaction should be executed)
- we want objects of different types
- we can use similar weights (wait time and queue length) to those used in the queue-matching problem to decide what to schedule
