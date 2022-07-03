# hdpsr
ICPP22hdpsr

## Installation
You should prepare a machine running Ubuntu16.04 and configure the Golang environment in advance.

To download the project:
```
git clone https://github.com/YuchongHu/hdpsr.git
```

Then download the module:
```
go mod tidy
```

## Project Architecture:
<!-- - `examples/main.go` contains the main func. For each run,  operate among "encode", "read", ... -->

- `erasure-global.go` contains the system-level interfaces and global structs and variables

- `erasure-init.go` contains the basic config file(`.hdr.sys`) read and write operation, once there is file change, we update the config file.

- `erasure-errors.go` contains the definitions for various possible errors.

- `erasure-encode.go` contains operation for striped file encoding, one great thing is that you could specify the data layout. 

- `erasure-layout.go` You could specific the layout, for example, random data distribution or some other heuristics. 

- `erasure-read.go` contains operation for striped file reading, if some parts are lost, we try to recover.

- `erasure-update.go` contains operation for striped file updating, if some parts are lost, we try to recover.

- `erasure-recover.go` deals with multi-disk recovery, concerning both data and meta data.

- `erasure-update.go` contains operation for striped file updating, if some parts are lost, we try to recover first.

- `fsr.go` contains operation for full-stripe-repair

- `psrap.go` contains operation for active preliminary partial-stripe-repair (single-disk repair)

- `psras.go` contains operation for active slower-first partial-stripe-repair (single-disk repair)

- `psrpa.go` contains operation for passive partial-stripe-repair (single-disk repair)

- `m-psrap.go` contains operation for active preliminary partial-stripe-repair (multi-disk repair)

- `m-psras.go` contains operation for active slower-first partial-stripe-repair (multi-disk repair)

- `m-psrpa.go` contains operation for passive partial-stripe-repair (multi-disk repair)


import:
[reedsolomon library](https://github.com/YuchongHu/reedsolomon)

## Usage
A complete demonstration of various CLI usage lies in `examples/build.sh`. You may have a glimpse.
Here we elaborate the steps as following, in dir `./examples`:

0. Build the project:
```
go build -o main ./main.go  
```

1. New a file named `.hdr.disks.path` in `./examples`, list the path of your local disks, e.g.,
```
/home/server1/data/data1
/home/server1/data/data2
/home/server1/data/data3
/home/server1/data/data4
/home/server1/data/data5
/home/server1/data/data6
/home/server1/data/data7
/home/server1/data/data8
/home/server1/data/data9
/home/server1/data/data10
/home/server1/data/data11
/home/server1/data/data12
/home/server1/data/data13
/home/server1/data/data14
/home/server1/data/data15
/home/server1/data/data16
```
Please remind that carriage return (CR), line feed (LF) are not allowed in the last line.

2. Initialise the system, you should explictly attach the number of data(k) and parity shards (m) as well as blocksize (in bytes), remember k+m must NOT be bigger than 256.
```
./main -md init -k 12 -m 4 -bs 4096 -dn 16
```
`bs` is the blockSize in bytes and `dn` is the diskNum you intend to use in `.hdr.disks.path`. Obviously, you should spare some disks for fault torlerance purpose.

3. Encode one examplar file.
```
./main -md encode -f {input file path} -o
```
4. decode(read) the examplar file.
```
./grasure -md read -f {input file path} -conStripes 100 -sp {output file path} 
```

5. check the hash string to see encode/decode is correct.

```
sha256sum {input file path}
```
```
sha256sum {output file path}
```

6. To delete the file in storage (currently irreversible, we are working on that):
```
./main -md delete -f {filebasename} -o
```

7. To update a file in the storage:
```
./main -md update -f {filebasename} -nf {local newfile path} -o
```

8. To recover a file via full-stripe-repair
```
./main -md fsr -fmd diskFail -fn 1 -f {input file path}
```
```
use `fn` to simulate the failed number of disks (default is 0), for example, `-fn 2` simluates shutdown of arbitrary two disks
```

9. To recover a file via partial-stripe-repair
```
./main -md psrap -fmd diskFail -fn 1 -f {input file path}
```
```
./main -md psras -fmd diskFail -fn 1 -f {input file path}
```
```
./main -md psrpa -fmd diskFail -fn 1 -f {input file path}
```

10. To recover a file with multi-disk failures via partial-stripe-repair
```
./main -md mpsrap -fmd diskFail -fn 1 -f {input file path}
```
```
./main -md mpsras -fmd diskFail -fn 1 -f {input file path}
```
```
./main -md mpsrpa -fmd diskFail -fn 1 -f {input file path}
```

## CLI parameters
the command-line parameters of `./examples/main.go` are listed as below.
|parameter(alias)|description|default|
|--|--|--|
|blockSize(bs)|the block size in bytes|4096|
|mode(md)|the mode of ec system, one of (encode, decode, update, scaling, recover)||
|dataNum(k)|the number of data shards|12|
|parityNum(m)|the number of parity shards(fault tolerance)|4|
|diskNum(dn)|the number of disks (may be less than those listed in `.hdr.disk.path`)|4|
|filePath(f)|upload: the local file path, download&update: the remote file basename||
|savePath|the local save path (local path)|file.save|
|newDataNum(new_k)|the new number of data shards|32|
|newParityNum(new_m)|the new number of parity shards|8|
|recoveredDiskPath(rDP)|the data path for recovered disk, default to /tmp/restore| /tmp/restore|
|override(o)|whether to override former files or directories, default to false|false|
|conWrites(cw)|whether to enable concurrent write, default is false|false|
|conReads(cr)|whether to enable concurrent read, default is false|false|
|failMode(fmd)|simulate [diskFail] or [bitRot] mode"|diskFail|
|failNum(fn)|simulate multiple disk failure, provides the fail number of disks|0|
|conStripes(cs)|how many stripes are allowed to encode/decode concurrently|100|
|quiet(q)|whether or not to mute outputs in terminal|false|
|log(l)|whether or not to write the repair time to a file called `log.txt`|false|

