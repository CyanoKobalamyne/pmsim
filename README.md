# Model vs reality
- read and write sets are distinct; write sets are much smaller
  - object popularity is distributed according to Zipf's Law
- there are a few kinds of transactions with fixed sizes (and times?)
- different objects have different sizes/costs
- execution time depends on scheduling decisions 
- memory bandwith can be a bottleneck too

# Transaction types
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
