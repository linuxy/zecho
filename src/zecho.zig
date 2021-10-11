const std = @import("std");
const flags = @import("flags.zig");
const atomic = @import("int.zig");
const io = std.io;
const fmt = std.fmt;
const debug = std.debug;
const warn = std.debug.warn;
const info = std.log.info;
const assert = std.debug.assert;
const builtin = std.builtin;
const net = std.net;
const os = std.os;

pub const log_level: std.log.Level = .warn;
var arg_duration: u32 = 0;
var arg_packetsize: u32 = 0;
var arg_count: i32 = 1;
var arg_address: [:0]const u8 = undefined;
var arg_port: u16 = 0;
var arg_numpacket: u64 = 0;
var arg_udp: bool = false;
var parsed_address: std.net.Address = undefined;
var found_port: bool = false;
var found_address: bool = false;
var total_sent: atomic.Int(u64) = atomic.Int(u64).init(0);
var total_recv: atomic.Int(u64) = atomic.Int(u64).init(0);
var total_good: atomic.Int(u64) = atomic.Int(u64).init(0);
var exiting: bool = false;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const allocator = &gpa.allocator;

const data = "hello";

const usage =
    \\zecho benchmark
    \\
    \\usage: zecho [ -a <address> ] [ -p <port> ] ( -t <send duration (ms)> ) ( -s <packet size> ) ( -c <parallel connections> ) ( -n <packets per con.> )
    \\       zecho (-h | --help)
    \\       zecho (-v | --version)
    \\
;

const build_version =
    \\zecho (zig echo) 0.1.0
    \\Copyright (C) 2021 Ian Applegate
    \\
    \\This program comes with NO WARRANTY, to the extent permitted by law.
    \\You may redistribute copies of this program under the terms of
    \\the GNU General Public License.
    \\For more information about these matters, see the file named COPYING.
    \\
;

pub fn main() !void {

    const argv: [][*:0]const u8 = os.argv;
    const result = flags.parse(argv[1..], &[_]flags.Flag{
        .{ .name = "--help", .kind = .boolean },
        .{ .name = "-h", .kind = .boolean },
        .{ .name = "--version", .kind = .boolean },
        .{ .name = "-v", .kind = .boolean },
        .{ .name = "-u", .kind = .boolean },        
        .{ .name = "--udp", .kind = .boolean },
        .{ .name = "-c", .kind = .arg },
        .{ .name = "-s", .kind = .arg },
        .{ .name = "-t", .kind = .arg },
        .{ .name = "-a", .kind = .arg },
        .{ .name = "-p", .kind = .arg },
        .{ .name = "-n", .kind = .arg },        
    }) catch {
        try io.getStdErr().writeAll(usage);
        os.exit(1);
    };
    if (result.boolFlag("--help") or result.boolFlag("-h")) {
        try io.getStdOut().writeAll(usage);
        os.exit(0);
    }
    if (result.args.len != 0) {
        std.log.err("unknown option '{s}'", .{result.args[0]});
        try io.getStdErr().writeAll(usage);
        os.exit(1);
    }
    if (result.boolFlag("--version") or result.boolFlag("-v")) {
        try io.getStdOut().writeAll(build_version);
        os.exit(0);
    }
    //TODO: implement
    if (result.boolFlag("-u") or result.boolFlag("--udp")) {
        arg_udp = true;
    }    
    if (result.argFlag("-a")) |address| {
        if(result.args.len == 0) {
            info("Found ip address: {s}", .{address});
            arg_address = std.mem.span(address);
            found_address = true;
        } else {
            try io.getStdErr().writeAll("Invalid argument for -a expected type [u8]\n");
            os.exit(1);
        }
    }
    if (result.argFlag("-p")) |port| {
        const maybe_port = fmt.parseInt(u16,  std.mem.span(port), 10) catch null;
        if(maybe_port) |int_port| {
            info("Found port address: {}", .{int_port});
            arg_port = int_port;
            found_port = true;
        } else {
            try io.getStdErr().writeAll("Invalid argument for -p expected type [u16]\n");
            os.exit(1);            
        }
    }
    if (result.argFlag("-c")) |count| {
        const maybe_count = fmt.parseInt(i32,  std.mem.span(count), 10) catch null;
        if (maybe_count) |int_count| {
            info("Found int count: {}", .{int_count});
            arg_count = int_count;
        } else {
            try io.getStdErr().writeAll("Invalid argument for -c expected type [i32]\n");
            os.exit(1);
        }
    }
    if (result.argFlag("-n")) |count| {
        const maybe_count = fmt.parseInt(u64,  std.mem.span(count), 10) catch null;
        if (maybe_count) |int_count| {
            info("Found int packet number: {}", .{int_count});
            arg_numpacket = int_count;
        } else {
            try io.getStdErr().writeAll("Invalid argument for -n expected type [u64]\n");
            os.exit(1);
        }
    }
    if (result.argFlag("-t")) |count| {
        const maybe_count = fmt.parseInt(u32,  std.mem.span(count), 10) catch null;
        if (maybe_count) |int_count| {
            info("Found int duration: {}", .{int_count});
            arg_duration = int_count;
        } else {
            try io.getStdErr().writeAll("Invalid argument for -t expected type [u32]\n");
            os.exit(1);
        }
    }
    //TODO: Implement
    if (result.argFlag("-s")) |count| {
        const maybe_count = fmt.parseInt(u32,  std.mem.span(count), 10) catch null;
        if (maybe_count) |int_count| {
            info("Found int packet size: {}", .{int_count});
            arg_packetsize = int_count;
        } else {
            try io.getStdErr().writeAll("Invalid argument for -s expected type [u32]\n");
            os.exit(1);
        }
    }
    if(found_address or found_port) {
        const arg_con = net.Address.parseIp(arg_address, arg_port) catch {
                try io.getStdErr().writeAll("Invalid address and/or port.\n");
                os.exit(1);        
        };
        info("Found valid port & address.", .{});
        parsed_address = arg_con;
    } else {
        if (os.argv.len - 1 == 0) {
            try io.getStdOut().writeAll(usage);
            os.exit(0);
        } else {
            try io.getStdErr().writeAll("Address and/or port not found.\n");
            os.exit(1);            
        }
    }

    info("Running...", .{});

    var i: usize = 0;
    var threads = ArrayList(std.Thread).init(allocator);

    while(i < arg_count) : (i+=1) {    
        _ = try threads.append(undefined);
    }
    std.debug.print("waiting for {} threads to finish.\n", .{i});

    var barrier = Barrier{};
    var rbarrier = Barrier{};
    defer {
        barrier.stop();
        rbarrier.stop();
        for (threads.items) |*tr| tr.join();
    }

    i = 0;
    var start = std.time.milliTimestamp();
    for (threads.items) |*tr| {
        tr.* = std.Thread.spawn(.{}, establish_connection, .{&barrier, &rbarrier}) catch unreachable;
        info("spawned thread {}\n", .{i});
        i+=1;
    }
    barrier.start();
    rbarrier.start();
    var duration: f64 = @intToFloat(f64, arg_duration);

    if(arg_duration >= 1000) {
        std.os.nanosleep(arg_duration / 1000, (arg_duration % 1000) * std.time.ns_per_ms);
    } else {
        std.os.nanosleep(0, arg_duration * std.time.ns_per_ms);
    }

    var end = std.time.milliTimestamp();
    std.debug.print("{} total packets sent, {} received, and {} matched in {}ms\n{} packets sent/s, {} packets received/s, {} matched goodput/s \n", .{
        total_sent.get(),
        total_recv.get(),
        total_good.get(),
        end - start,
        @floatToInt(u64, @divTrunc(@intToFloat(f64, total_sent.get()), duration / 1000)),
        @floatToInt(u64, @divTrunc(@intToFloat(f64, total_recv.get()), duration / 1000)),
        @floatToInt(u64, @divTrunc(@intToFloat(f64, total_good.get()), duration / 1000)),
    });
}

const Barrier = struct {
    state: std.atomic.Atomic(u32) = std.atomic.Atomic(u32).init(0),

    fn wait(self: *const Barrier) void {
        while (self.state.load(.Acquire) == 0) {
            std.Thread.Futex.wait(&self.state, 0, null) catch unreachable;
        }
    }

    fn isRunning(self: *const Barrier) bool {
        return self.state.load(.Acquire) == 1;
    }

    fn wake(self: *Barrier, value: u32) void {
        self.state.store(value, .Release);
        std.Thread.Futex.wake(&self.state, std.math.maxInt(u32));
    }

    fn start(self: *Barrier) void {
        self.wake(1);
    }

    fn stop(self: *Barrier) void {
        self.wake(2);
    }
};

fn establish_connection(noalias barrier: *const Barrier, noalias rbarrier: *const Barrier) !void {

    var stream: std.net.Stream = undefined;

    barrier.wait();
    while(barrier.isRunning()) {
        info("barrier is running", .{});
        stream = std.net.tcpConnectToAddress(parsed_address) catch|err|{
            warn("unable to connect: {} ; retrying\n", .{err});
            std.time.sleep(2_000_000_000);
            continue;
        };
        break;
    }
    try send_data(&stream, rbarrier);    
    defer stream.close();
}

fn send_data(net_stream: *const std.net.Stream, noalias rbarrier: *const Barrier) anyerror!void {
    
    var sent: u64 = 0;
    var loop: u64 = 0;
    var serr: u64 = 0;
    var runtime: i64 = 0;

    var match = &(try std.Thread.spawn(.{}, matcher, .{net_stream, rbarrier}));

    var start = std.time.milliTimestamp();
    while (true) {

        var to_send = data;
        if(net_stream.write(to_send[0..])) |err| {
            _ = err;
            sent += 1;
            _ = total_sent.incr();
        } else |err| switch (err) {
            error.ConnectionResetByPeer => {break;},
            error.Unexpected => {serr += 1;},
            error.DiskQuota => {serr += 1;},
            error.FileTooBig => {serr += 1;},
            error.InputOutput => {serr += 1;},
            error.NoSpaceLeft => {serr += 1;},
            error.AccessDenied => {serr += 1;},
            error.BrokenPipe => {serr += 1;},
            error.SystemResources => {serr += 1;},
            error.OperationAborted => {serr += 1;},
            error.NotOpenForWriting => {serr += 1;},
            error.WouldBlock => {serr += 1;},
        }
        loop += 1;

        //Only check time every 10k packets ~ 3ms
        if((std.math.mod(u64, loop, 10000) catch unreachable) == 0) {
            runtime = std.time.milliTimestamp(); 
        }
        if((arg_numpacket > 0) and (loop >= arg_numpacket)) {
            info("exited number. {}", .{std.Thread.getCurrentId()});
            break;
        }
        if((arg_duration > 0) and (runtime >= (start + arg_duration))) {
            info("exited duration. {}", .{std.Thread.getCurrentId()});
            break;
        }
    }

    match.detach();

    var end = std.time.milliTimestamp();

    info("{} packets sent in {}ms with {} errors from thread {}\n", .{
        sent,
        end - start,
        serr,
        std.Thread.getCurrentId(),
    });
}

fn matcher(net_stream: *const std.net.Stream, noalias rbarrier: *const Barrier) anyerror!void {

    var recv: u64 = 0;
    var unmatched: u64 = 0;
    var start = std.time.milliTimestamp();
    var match: u64 = 0;

    rbarrier.wait();
    while (rbarrier.isRunning()) {
        var buf: [data.len]u8 = undefined;
        var bytes = net_stream.read(buf[0..]) catch break;

        if(bytes == -1) {
            break;
        } else {
            recv += 1;
            _ = total_recv.incr();
        }

        if(std.mem.eql(u8, buf[0..], data)) {
            match += 1;
            _ = total_good.incr();
        } else {
            unmatched += 1;
        }
    }
    info("exited matcher.", .{});
    var end = std.time.milliTimestamp();

    info("{} packets received, {} matched and {} unmatched in {}ms from thread {}\n", .{
        recv,
        match,
        unmatched,
        end - start,
        std.Thread.getCurrentId(),
    });
}