# zecho
A zig echo server benchmark.

```
$ zig build
$ ./zig-out/bin/zecho --help

usage: zecho [ -a <address> ] [ -p <port> ] ( -t <send duration (ms)> ) ( -s <packet size> ) ( -c <parallel connections> ) ( -n <packets per con.> ) ( --uring )
       zecho (-h | --help)
       zecho (-v | --version)
```

Running zecho:
```
$ ./zig-out/bin/zecho -a 127.0.0.1 -p 9999 -c 10 -t 2000
waiting for 10 threads to finish.
6000274 total packets sent, 4475510 received, and 4109226 matched in 2002ms
3000137 packets sent/s, 2237755 packets received/s, 2054613 matched goodput/s
```
