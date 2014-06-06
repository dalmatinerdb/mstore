mstore
======
MStore is a experimental metric store build in erlang, the primary functions are `open`, `new`, `get` and `put`.

A datastore is defined by:
 * The size of the consistant hashing ring.
 * The number of entreis per metrics.
 * The initial offset.
 
For each chunk a index is created (defining the position of the metrics) and a datafile which holds the values. This makes reading a number of metrics as simple as a calculation and a sequential read.


For a store holding 1000 metrics writing to the the numbers 0-999 would be in the file 0, 1000-1999 would be in the second file etc.
