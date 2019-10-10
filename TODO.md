# Current
- read paper sent by Ziv
- investigate better-than-perfect parallelism
- parametrize over object pool size, transaction time, and Zipf's Law parameter
- we want a transaction template to run with
- add transactions to constant-size pool while running
- choose transaction from fixed-size pool

# Long-term
- queue multiple transactions from scheduler
- decouple scheduler from executor (which decides which queued transaction should be executed)
- we want objects of different types
