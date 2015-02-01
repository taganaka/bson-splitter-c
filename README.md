# bson-splitter-c
Pure C BSON file splitter

## From already existing BSON file ##
```
$ ./bson-splitter dump/test/test.bson 128
[split-1.bson] bytes written: 134221809 docs dumped: 11676
[split-2.bson] bytes written: 134220283 docs dumped: 55885
[split-3.bson] bytes written: 124087704 docs dumped: 63990
$
```

## From STDIN ##
```
$ mongodump -d test -c test -o - | ./bson-splitter - 128
connected to: 127.0.0.1
[split-1.bson] bytes written: 134221809 docs dumped: 11676
[split-2.bson] bytes written: 134220283 docs dumped: 55885
[split-3.bson] bytes written: 124087704 docs dumped: 63990
$
```
