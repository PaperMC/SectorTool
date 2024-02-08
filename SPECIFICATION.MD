# Introduction

This specification will first review the RegionFile format
followed by specifying the SectorFile format and the improvements
it makes to RegionFile.

# RegionFile Format (".mca", ".mcc")

The RegionFile format is the current format for which chunk
data is stored on disk. This section describes the history
of the RegionFile format, the details of how chunk data is 
stored in the RegionFile, and its current deficiencies.

## History

The RegionFile format (by Scaevolus) was introduced to Minecraft 
to eliminate performance problems with the prior format, the Alpha Format.

### Alpha Format

The Alpha Format dynamically allocates a single file per chunk 
in the file system, which served to facilitate Minecraft's dynamic
generation and loading of chunks. 

#### Performance

The choice to allocate a single file per chunk lead to performance problems.
Specifically, the cost of opening/closing files to read or write chunks 
is a significant. Since Minecraft generally loads/saves chunks in
particular areas, rather than randomly spread out, it is possible to 
amortize the cost of opening/closing files (and possibly seeking) by 
storing neighbouring chunks together in a single file. As a result, the
RegionFile disk format was born - which did largely resolve the performance
issues.

## Disk Format

The RegionFile format is a file-based format comprised of a header and 
data section. Its sector length is 4096 bytes, which is typically the 
sector size of most file systems. The RegionFile addresses data
by sector size. Each RegionFile stores 32x32 chunks. The (modern) 
format stores chunk data externally in their own files when the data size 
exceeds 255 sectors.

### Header

The header is exactly two sectors in length, with the first sector
being the "location" data and the second sector being the "timestamp"
data. Each of these sectors comprises an array of 1024 4-byte 
big-endian integer values. The index into each array is determined 
uniquely by chunk coordinate, effectively creating a mapping of
chunk coordinate to its location and timestamp.

The location data for a given chunk coordinate is encoded into an integer,
which represents the length in sectors (lower 8 bits) and an offset
in the RegionFile in sectors (upper 24 bits) starting at the beginning
of the file (with sector offset 0 pointing to the header). If the location
is 0 (representing a 0 length and 0 offset), then the chunk is considered
absent without any stored data. If the location is non-zero, then 
the chunk's data begins exactly at the first byte of the sector offset,
and ends before or at the last byte of the offset+length sector.

The timestamp data represents the last time (currentTimeMillis / 1000) that
a chunk was written at.

### Chunk Data

Chunk data is stored beginning at a sector, with a small header
being the first 5 bytes.

The first 4 bytes represent a big-endian integer which indicate
the size of the compressed chunk data plus 1, which we will call
"n".

The 1-byte value that follows represents the "version" of encoding
of the chunk data, which is one of the following:

1. GZIP (1)
   
    Represents data compressed using GZIP.

2. DEFLATE (2)

    Represents data compressed using DEFLATE.

3. NONE (3)
    
    Represents data stored without compression.

4. LZ4 (4)

    Represents data compressed with LZ4.

The n-1 remaining bytes represent the compressed chunk data.

The version id may be bitwise-ORed with 128, indicating that the 
compressed chunk data is stored in an external file with the 
following format: c.chunkX.chunkZ.mcc

### Deficiencies

There are currently a couple of deficiencies in the RegionFile
format and its implementation. One problem is that the size
of most chunk data is well below the sector size of 4096 bytes, which
leads to inefficient allocation of file space. The other problem is that
RegionFile relies heavily on the header section to map chunk data, which
leads to significant data loss if the header is corrupted.

#### Inefficient usage of allocated sectors

Most chunk data compressed using DEFLATE, which is the default compression
type, is well below the sector size (4096 bytes). Specifically, new chunk 
data generated in the overworld and nether is typically 2/3rds of the 
sector size. As a result, worlds are typically much larger than the total
sum of the chunk data even after accounting for fragmentation of the 
RegionFile.

It should be noted that this issue is not specific to the RegionFile format,
as it was present in the Alpha Format as most file systems have a sector
size of 4096 bytes as well. As a result, the file size comparison between
Alpha and RegionFile was minimal. Rather, the move to the RegionFile 
format simply missed an opportunity to amortize the cost of allocating 
file system sectors because RegionFile uses a sector size of 4096 bytes.

#### Reliability issues

The RegionFile format relies on the header section to identify
where chunk data is inside the file. If the header is corrupted, 
then valid chunk data is lost as the header is required to find it.
Since the chunk data is still valid, it is possible to scan the file
and rebuild the header ("recalculation" operation).
However, recalculation only works when the chunk data itself contains its
own position (i.e. chunk data, entity data, but not poi data).
Additionally, the Vanilla RegionFile code does not attempt any recalculation
logic. As a result, in Vanilla, any corruption of the header results in 
data loss.

Paper includes logic to recalculate the header, however it only works for 
block data (i.e. not entity, not poi). The recalculation logic also requires
fully parsing potential chunk data, as that is the only "integrity" check
it can perform.

## SectorFile Format (".sf", ".sfe")

The SectorFile format is a file-based format comprised of a file header and
data section. Its sector length is 512 bytes, and it addresses data by 
sector size. SectorFiles store 32x32 chunks for 42 separately addressable 
types of chunk data (identified as integer values from [0,41]), resulting
in data being addressed by chunk coordinate and data type. Currently, 
there are three separate types of data:block (sometimes called "chunk"), 
entity, and poi. Like RegionFile, SectorFile will store data in 
external files when the data size exceeds a maximum amount. Unlike 
RegionFile, SectorFile may store data externally if the SectorFile
runs out of sectors to allocate internally.

### File Header

The file header is composed of 3 contiguous sections: the hash 
(8-byte big-endian integer), 42 type header hashes (8-byte big endian 
integers), and the 42 header sector offsets (4-byte big-endian integer). 
The hash represents the XXHash64 value of the type header hashes 
and the type header offsets. The type header hashes represent the XXHash64 
values for the type header data, which is described below. The type 
header offsets represent the offset in sectors from the beginning of the 
file for where the type header data is stored.

The sector file is named `sectionX.sectionZ.sf`, where sectionX and 
sectionZ represent the region coordinates of a chunk. The region coordinates
of a chunk is simply `coordinate >> SectorFile.SECTION_SHIFT`.

### Type Header

The type header is composed of 32x32 locations representing both the offset 
and length in sectors of a particular type of chunk data. The type header
is allocated dynamically, as its own location is determined by an offset
in the header. As a result, type headers may be deleted or created at 
any time - allowing new data types to be added to existing SectorFiles
seamlessly.

The type header array is indexed by a chunk coordinate (x, z) where
`0 <= x,z < SECTION_SIZE` where SECTION_SIZE = 32. The value to index the
type header is determined by `x | (z * SECTION_SIZE)`, which is referred 
to as the index.

Each location is a 4-byte big-endian integer with the upper 22 bits reserved 
for the sector offset in the file and the lower 10 bits reserved for the
sector length in the file. This allows the total file size to be 
approximately ~2GB and support up to ~0.5MB data lengths. 

### Data Format

The data format is composed of a data header followed by compressed data. 
The data header is 32 bytes, composed of the following in order:
8-byte XXHash64 of the following header data, 8-byte XXHash64 of the
compressed data, 8-byte integer representing the value of 
System#currentTimeMillis at the time of writing, 4-byte integer representing
the length in bytes of the compressed data, 2-byte integer representing the
index value (see Type Header), 1-byte integer representing the data type id,
and finally a 1-byte integer representing the compression type id. All integer
types are big-endian encoded.

### Compression Types

There are five supported compression types:

1. GZIP (id = 1)
2. DEFLATE (id = 2)
3. NONE (id = 3, does not compress)
4. LZ4 (id = 4)
5. ZSTD (id = 5)

Below are stats comparing compression speed, decompression speed, and compression ratio
taken for the [hermitcraft season 9 world](https://download.hermitcraft.com/hermitcraft9.zip) 
overworld block data:

| test                     | GZIP   | DEFLATE | LZ4   | ZSTD   |
|--------------------------|--------|---------|-------|--------|
| avg compress time (µs)   | 934.38 | 909.19  | 57.63 | 190.95 |
| avg decompress time (µs) | 114.39 | 123.87  | 26.83 | 53.42  |
| avg compress ratio       | 7.43   | 7.44    | 4.35  | 6.76   |

Tested on a 7950X. 

ZSTD shows the best tradeoff between speed and compression ratio, and as a 
result it is the default compression type.

### External Allocation

SectorFile will store data externally in one of two circumstances: the data
is too large to store internally, or the SectorFile does not have enough sectors
internally to allocate. When data is stored externally, the location for the data
in the type header is a special constant which does not point to any internal 
allocation. As a result, the data header and compressed data is stored entirely
in the external file - unlike RegionFile, where the external file is just the compressed data.

The external file is named `chunkX.chunkZ-typeId.sfe`, where `chunkX` and 
`chunkZ` are the absolute chunk coordinates, and `typeId` is the id of the 
data type.

The data is too large to store internally when it exceeds the maximum
length encode-able in the type header location, which is around ~0.5MB. As such,
it must be stored externally.

The data may also be stored externally when SectorFile runs out of sectors
to allocate internally. SectorFile allows 32x32x42 = 43,008 different 
data items to be stored. If each data item is sized at the maximum internal
allocation of ~0.5MB, then the total size is ~21GB - which exceeds the maximum
addressable sectors by the type header (max of ~2GB). As a result, the only
place to store data is externally when the SectorFile runs out of sectors.

### Fixes to RegionFile Deficiencies

#### Inefficient usage of allocated sectors

In order to show that sectors are utilised more efficiently, a test was conducted
on RegionFiles from a freshly generated world (with seed = "sectors", and render distance
= 32) and a more established world, the [hermitcraft season 9 world](https://download.hermitcraft.com/hermitcraft9.zip). The freshly 
generated world is expected to have smaller chunk data compared to the more 
established worlds. As a result, testing these two worlds gives some idea of a lower
and upper bound on the expected efficiency of disk space.

The test sums up the total bytes of compressed data using both ZSTD and DEFLATE for 
each chunk, and then divide this value by the sum of total bytes of compressed data 
rounded up to the nearest sector. 

The resulting value is within [0, 1] and represent the efficiency of the
allocated sectors for a given sector size and compression type.

The results are tabulated below, with each compression type having the sector size
shown in parentheses.

| world                     | DEFLATE (512) | ZSTD (512) | DEFLATE (4096) | ZSTD (4096) |
|---------------------------|---------------|------------|----------------|-------------|
| hermitcraft (~1GB)        | 0.96          | 0.97       | 0.74           | 0.77        |
| freshly generated (~31MB) | 0.95          | 0.95       | 0.58           | 0.61        |

For DEFLATE, the efficiency improvements range from 1.30x up to 1.64x.

For ZSTD, the efficiency improvements range from 1.26x up to 1.56x.

Note: In all cases, 512 byte sectors show >=95% efficiency which indicate that a smaller
sector size is unlikely to give any value in terms of disk space.


#### Reliability issues

SectorFile resolves the dependency that RegionFile has on its header by including
logic to recalculate the header data directly from the SectorFile / external file(s). 
The recalculation logic scans the internal file and external files to rebuild the 
file header and the type headers so that they only point to valid data.

The header data is possible to recalculate directly from the stored data because the
data headers include the following fields:

1. Data header XXHash64, used to validate integrity of the data header (not present in RegionFile)
2. Data XXHash64, used to validate integrity of the compressed data (not present in RegionFile)
3. Compressed data size field, used to determine the length of the data (present in RegionFile)
4. Data type id, used to determine which type the data is (not relevant for RegionFile)
5. Last written timestamp, used to determine which data is newest (not present in RegionFile)
6. Data index, used to determine which chunk coordinate the data belongs to (not present in RegionFile)

On one hand, these fields allow any stored data to be validated and allow 
the recalculation logic to determine what chunk coordinate and data type 
it belongs to - which is all it needs to rebuild the type headers. On the 
other hand, RegionFile does not contain any such fields and as a result 
cannot perform recalculation logic unless the compressed data contains 
similar fields (only which block data contains).

Additionally, recalculation logic is run any time an inconsistency is detected.

An inconsistency is detected, which begins recalculation, when any of the following 
conditions are met:
1. Mismatching file header XXHash64
2. Any mismatching type header XXHash64
3. Mismatching data header XXHash64
4. Mismatching compressed data XXHash64
5. Invalid type/file header data: negative sector offsets, sector offsets exceeding 
   file size, overlapping sector allocations
6. Invalid data header on read(): mismatch of compressed size, mismatch of encoded index,
   mismatch of the data type

It is entirely possible to delegate recalculation logic to external tools, as
has happened with (Vanilla) RegionFile. However, delaying the recalculation logic
opens timing windows for real data to be overwritten and lost. Effectively, built-in
recalculation logic minimizes effects of, but does not eliminate (users still need backups), 
data loss.
