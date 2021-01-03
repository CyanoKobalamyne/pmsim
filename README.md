# Puppet Master

Puppet Master is a hardware-based distributed scheduler. The code here is its
software simulation.

## Requirements
- Python 3.9
- [more-itertools](https://github.com/more-itertools/more-itertools)
- [matplotlib](https://matplotlib.org/) (optional, for plotting commands)

## Model

- Transaction-based scheduling.
- Read and write sets are distinct. Write sets are generally smaller.
- Object popularity is distributed according to Zipf's law.
- There are a few kinds of transactions with fixed sizes (and times?).
- Different objects have different sizes/costs.
- Execution time depends on scheduling decisions.
- Memory bandwith can be a bottleneck too.

## Transaction types

1. Key-Value Store:
   - 1 read (get)
   - 1 write (set)
1. Bookstore:
   - 1 read (query)
   - 1 read + 1 write (add-to-cart)
   - N writes (commit)
1. Bank:
   - 1 read (query)
   - 1 write (deposit)
   - 2 writes (transfer)
