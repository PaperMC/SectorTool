# SectorTool

SectorTool primarily performs conversion and deconversion of worlds from/to
the SectorFile format. It can also perform benchmarks or other analysis of
RegionFiles.

It's highly recommended to only use this tool on **COPIES** of worlds.

The test branch does not contain any conversion code, you **MUST** use this tool
to convert.

This tool does not attempt to fix errors in RegionFiles. Make sure your world
has no errors in your RegionFiles before converting.

SectorTool can perform one of these operations:
1. [Convert to SectorFile](#conversion-to-sectorfile-from-regionfile)
2. [Deconvert from SectorFile](#conversion-from-sectorfile-to-regionfile-deconversion)
3. [Verify consistency from RegionFile to SectorFile](#verification-test)
4. [Perform compression tests on RegionFiles](#compression-test)
5. [Read results from compression tests](#read-compression-test)
6. [Analyse sector allocation for RegionFiles](#analyse-regionfiles)

See [the specification](SPECIFICATION.MD) for exact details on the SectorFile
format.

## World Layout

Described are the folder layouts of world data in Vanilla and Bukkit.

### Vanilla

Vanilla has the overworld under the `world` directory, with its data being placed
directly in the `world` folder. The nether's data is placed under `world/DIM-1`,
the end's data is placed under `world/DIM1`.

### Bukkit (Spigot/Paper/Folia)

If you run Bukkit, Spigot, Paper, or Folia your worlds are formatted in the "Bukkit"
layout. The Bukkit layout places the overworld in the `world` folder in the root
server directory. The nether is placed in the `world_nether` folder in the root
server directory, however its data (excluding level.dat and friends) is placed
under `world_nether/DIM-1`, similar to how Vanilla would store under `world/DIM-1`.
The end is placed in the `world_the_end` folder in the root server directory, 
with its data being stored in `world_the_end/DIM1`.

### Changes

Instead of data being stored under `entities`, `poi`, and `region` data will
only be stored under the `sectors` directory. SectorFiles contain entities, 
poi, and region data under one SectorFile rather than multiple RegionFiles.

## Primary Usages

### Conversion to SectorFile from RegionFile

Converts a single world to the SectorFile format. The old RegionFiles
will not be deleted (folders: entities, poi, region), it is recommended 
that you move them after conversion. By moving the old data once you have
converted, it will be easier to convert back to RegionFile.

#### Behavior for already converted worlds

Additionally, there are no checks in-place to detect worlds already converted. 
Conversion will simply write the data found in the RegionFiles to the SectorFiles,
so if the SectorFiles have additional data for chunks not present in the original
RegionFiles they will be unaffected - but the chunks present in the RegionFiles
will be copied.

If you do not want this merging behavior then delete the `sectors` folder.

#### Command and options

```shell
java -Dop=conv \
 -Dthreads=<threads> \
 -Dinput="<input>" \
 -Dcompress=<compress type id> \
 -jar sectortool.jar
```

#### threads (optional)

Specified number of threads to use. Use 1 for HDDs, use larger amounts
for SSDs. Defaults to 1/2 of available processors.

#### input

Input root world directory. For Vanilla worlds, this is plainly just the 
root world directory. For Bukkit worlds, you will want to run the tool for 
each dimension, using the root world directory as input (i.e. point to 
`world`, `world_nether`, and `world_the_end`) for each dimension.

#### compress

The compression type to use on the new SectorFiles. Values:
* GZIP (id = 1)
* DEFLATE (id = 2)
* NONE (id = 3)
* LZ4 (id = 4)
* ZSTD (id = 5)

DEFLATE (id = 2) is the current default. LZ4 (id = 4) is fast but not space 
efficient. ZSTD (id = 5) is fast and slightly less space efficient than DEFLATE.


### Conversion from SectorFile to RegionFile (Deconversion)

Converts a world from SectorFile format back to RegionFile format. It is 
highly recommended that you ensure that no RegionFiles are in the current world
(just move them to a separate directory), as the original conversion process from
RegionFile to SectorFile done by this tool **WILL NOT** delete RegionFiles. Note 
that the converted SectorFiles will not be deleted, you will need to do that.

#### Command and options

```shell
java -Dop=deconv \
 -Dthreads=<threads> \
 -Dinput="<input>" \
 -Dcompress=<compress type id> \
 -jar sectortool.jar
```

#### threads (optional)

Specified number of threads to use. Use 1 for HDDs, use larger amounts
for SSDs. Defaults to 1/2 of available processors.

#### input

Input root world directory. For Vanilla worlds, this is plainly just the
root world directory. For Bukkit worlds, you will want to run the tool for
each dimension, using the root world directory as input (i.e. point to
`world`, `world_nether`, and `world_the_end`) for each dimension.

#### compress

The compression type to use on the RegionFiles. Values:
* GZIP (id = 1)
* DEFLATE (id = 2)
* NONE (id = 3)
* LZ4 (id = 4)

DEFLATE (id = 2) is the current default for Vanilla. LZ4 (id = 4) is 
**NOT** supported outside the latest snapshot, so do **NOT** use LZ4 
unless it is **FOR** snapshot worlds.

Note that ZSTD is not present because Vanilla RegionFile does not support ZSTD.

### Verification Test

Verifies that all data contained in the RegionFiles are contained in
the SectorFiles. This will not detect extra data in the SectorFiles.

#### Command and options

```shell
java -Dop=verify \
 -Dthreads=<threads> \
 -Dinput="<input>" \
 -Dcompress=<compress type id> \
 -jar sectortool.jar
```

#### threads (optional)

Specified number of threads to use. Use 1 for HDDs, use larger amounts
for SSDs. Defaults to 1/2 of available processors.

#### input

Input root world directory. For Vanilla worlds, this is plainly just the
root world directory. For Bukkit worlds, you will want to run the tool for
each dimension, using the root world directory as input (i.e. point to
`world`, `world_nether`, and `world_the_end`) for each dimension.

#### compress (required but unused, pick one)

Values:
* GZIP (id = 1)
* DEFLATE (id = 2)
* NONE (id = 3)
* LZ4 (id = 4)
* ZSTD (id = 5)

## Secondary Usages

### Compression Test

Performs compression tests (time to decompress, compress, and resulting 
compression ratios) and store the results in a file which may be later 
used by the [Compression Read](#read-compression-test) operation.

```shell
java -Dop=test \
 -Dthreads=<threads> \
 -Dinput="<input>" \
 -Doutput="<output>" \
 -Dmax_regions=<max regions> \
 -jar sectortool.jar
```

#### input

Input directory of RegionFiles to run tests on. This is not a world directory,
it is a directory containing `.mca` files.

#### output

File to store test results in. This file should not exist, the tool will exit
if it does.

#### max_regions (optional)

The maximum number of RegionFiles to test on. Defaults to infinity.

#### threads (optional)

Specified number of threads to use. Use 1 for HDDs, use larger amounts
for SSDs. Defaults to 1/2 of available processors.


### Read Compression Test

Reads result file from a compression test, converting the data to some 
summary stats printed to stdout as well as converting the results to raw data
printed to `results.psv` in the working directory.


```shell
java -Dop=read \
 -Dinput="<input>" \
 -jar sectortool.jar
```

#### input

Results file generated by the compression test.

### Analyse RegionFiles

Computes the total number of 4096 byte sectors used by RegionFiles on disk 
("file sectors"), the number of 4096 byte sectors allocated to live data by 
RegionFiles on disk ("allocated sectors"), the theoretical number of 512 byte 
sectors allocated ("alternate allocated sectors"), the total size of compressed 
data in bytes ("total data size"), and the number of obvious (header offsets/length
are invalid or point outside of file) errors in RegionFile headers ("errors").

Computing `allocated sectors / file sectors` gives some indication of RegionFile
fragmentation.

Computing `allocated sectors / alternate allocated sectors * (4096/512)` gives
some indication of the improvements expected to come by using SectorFile.

#### Command and options

```shell
java -Dop=analyse \
 -Dthreads=<threads> \
 -Dinput="<input>" \
 -jar sectortool.jar
```

#### threads (optional)

Specified number of threads to use. Use 1 for HDDs, use larger amounts
for SSDs. Defaults to 1/2 of available processors.

#### input

Input root world directory. For Vanilla worlds, this is plainly just the
root world directory. For Bukkit worlds, you will want to run the tool for
each dimension, using the root world directory as input (i.e. point to
`world`, `world_nether`, and `world_the_end`) for each dimension.