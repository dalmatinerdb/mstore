## mstore

[![Build Status](http://quickcheck-ci.com/p/licenser/mstore.png)](http://quickcheck-ci.com/p/licenser/mstore)

MStore is a experimental metric store build in erlang, the primary functions are `open`, `new`, `get` and `put`.

A datastore is defined by:
 * The size of the consistant hashing ring.
 * The number of entreis per metrics.
 * The initial offset.

For each chunk a index is created (defining the position of the metrics) and a datafile which holds the values. This makes reading a number of metrics as simple as a calculation and a sequential read.

For a store holding 1000 metrics writing to the the numbers 0-999 would be in the file 0, 1000-1999 would be in the second file etc.


## Idea

The basic idea is to take advantage of the special characteristics metrics have and modern filesystems. The following assumptions about metrics and filesystems are taken:

* Metrics occour in a regular interval (i.e. every second) skips happen but are rare
* Metrics are immutable. (i.e. once the cpu temperature was recorded for a measurement period it won't ever change again).
* Reads are highly sequential, 'give me the values between X and Y'.
* Metrics are written nearly sequentially, the delta of time between two metrics written will propably be small, this allows to limit the amount of open files.
* Metrics can be represented as 64bit integers. (this might change!)
* The filesystem uses checksums for data, this means we don't need to cehcksum values.
* The filesystem allows compression. This means longer stratches of non written metrics don't have a big impact since a bunch fo 0's on the FS will easiely be compressed away.
* The filesystem has a decent cacheing strategy (no need for mmap nonsense).
* The filesystem actually is ZFS.

## File Layout

### Set

A set allows to group metrics into a hash ring, this limits the size of single files open. The directory layout will be like this:

```
<base dir>/<chash key>/<offset>.{mstore,idx} - data and store index files
<base dir>/mstore - set index file
```

#### Index File (for a set)

The index file is simply a Erlang file that can be read via consult:

```
{FileSize, CHashSize, Seed, Metrics}.
```
* FileSize: The number of points per metric stored in the file.
* CHashSize: The number of elements in the CHash ring.
* Seed: A seed used to hash the metric keys, this is needed to allow putting a set behind another CHash ring (i.e. riak core). W/o the seed the distribution would not be even.
* Metrics: A list of all metrics stored in this set, used for looking up metrics.

### Store
#### Data File

Currently data is fixed to 64 bit (8 byte) integers this means a data file is layed out like this:

```
<metric 1:FileSize*8><metric 2:FileSize*8><metric 2:FileSize*8>
```

#### Index File (for a metric)

The index file is simply a Erlang file that can be read via consult:
```
{Offse, FileSize, [{Metric, Index}]}.
```

* Offset: the base offset of the file.
* FileSize: The number of points per metric stored in the file.
* Metric and Index: A list of metricses and their indexes in the file.
