const std = @import("std");
const flags = @import("flags.zig");
const atomic = @import("int.zig");
const io = std.io;
const fmt = std.fmt;
const debug = std.debug;
const warn = std.debug.warn;
const info = std.log.info;
const assert = std.debug.assert;
const builtin = @import("builtin");
const net = std.net;
const os = std.os;

pub const log_level: std.log.Level = .warn;
var arg_duration: u32 = 0;
var arg_packetsize: u32 = 0;
var arg_count: u64 = 1;
var arg_address: [:0]const u8 = undefined;
var arg_port: u16 = 0;
var arg_numpacket: u64 = 0;
var arg_udp: bool = false;
var arg_uring: bool = false;
var parsed_address: std.net.Address = undefined;
var found_port: bool = false;
var found_address: bool = false;
var total_sent: atomic.Int(u64) = atomic.Int(u64).init(0);
var total_recv: atomic.Int(u64) = atomic.Int(u64).init(0);
var total_good: atomic.Int(u64) = atomic.Int(u64).init(0);
var sent_pps = std.atomic.Atomic(usize).init(0);
var recv_pps = std.atomic.Atomic(usize).init(0);
var exiting: bool = false;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const allocator = &gpa.allocator;

var data: []u8 = undefined;
var data_len: usize = 0;

const usage =
    \\zecho benchmark
    \\
    \\usage: zecho [ -a <address> ] [ -p <port> ] ( -t <send duration (ms)> ) ( -s <packet size> ) ( -c <parallel connections> ) ( -n <packets per con.> ) ( --uring )
    \\       zecho ( -h | --help )
    \\       zecho ( -v | --version )
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

const Event = packed struct {
    fd: i32,
    op: Op,
};

const Op = enum(u32) {
    Accept,
    Recv,
    Send,
};

pub fn main() !void {

    const argv: [][*:0]const u8 = os.argv;
    const result = flags.parse(argv[1..], &[_]flags.Flag{
        .{ .name = "--help", .kind = .boolean },
        .{ .name = "-h", .kind = .boolean },
        .{ .name = "--version", .kind = .boolean },
        .{ .name = "-v", .kind = .boolean },
        .{ .name = "-u", .kind = .boolean },        
        .{ .name = "--udp", .kind = .boolean },
        .{ .name = "--uring", .kind = .boolean },
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
    if (result.boolFlag("--uring")) {
        arg_uring = true;
        if (builtin.os.tag != .linux) return error.LinuxRequired;
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
        const maybe_count = fmt.parseInt(u64,  std.mem.span(count), 10) catch null;
        if (maybe_count) |int_count| {
            info("Found int count: {}", .{int_count});
            arg_count = int_count;
        } else {
            try io.getStdErr().writeAll("Invalid argument for -c expected type [u64]\n");
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

    var prng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        std.os.getrandom(std.mem.asBytes(&seed)) catch {
            warn("OS getRandom failed.", .{});
            std.process.exit(1);
        };
        break :blk seed;
    });
    const rand = &prng.random;

    if(arg_packetsize > 0) {
        var i: usize = 0;
        data = try allocator.alloc(u8, arg_packetsize);
        while (i < arg_packetsize) : (i += 1) {
            data[i] = rand.intRangeLessThan(u8, 1, 255);
            data_len += 1;
        }
        info("data: {s}", .{data});
    } else {
        data = try allocator.alloc(u8, 5);
        data[0] = 'h';
        data[1] = 'e';
        data[2] = 'l';
        data[3] = 'l';
        data[4] = 'o';
        data_len = 5;
    }
    info("Running...", .{});

    if(!arg_uring) {
        try standard_setup();
    } else {
        if(arg_count > 4) {
            arg_count = 4;
        }
        try uring_setup();
    }

}

fn standard_setup() !void {
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

    if(arg_duration >= 1000) {
        std.os.nanosleep(arg_duration / 1000, (arg_duration % 1000) * std.time.ns_per_ms);
    } else {
        std.os.nanosleep(0, arg_duration * std.time.ns_per_ms);
    }

    if(arg_numpacket > 0) {
        while((total_sent.get() / arg_count) < arg_numpacket) {
            std.os.nanosleep(0, 100_000);
        }
    }

    var end = std.time.milliTimestamp();
    var duration: f64 = @intToFloat(f64, end - start);

    info("total sent {} {}\n", .{total_sent.get(), duration});
    std.debug.print("{} total packets sent, {} received, and {} matched in {}ms\n{} packets sent/s, {} packets received/s, {} matched goodput/s @ {} bytes\n", .{
        total_sent.get(),
        total_recv.get(),
        total_good.get(),
        end - start,
        @floatToInt(u64, @divTrunc(@intToFloat(f64, total_sent.get()), duration / 1000)),
        @floatToInt(u64, @divTrunc(@intToFloat(f64, total_recv.get()), duration / 1000)),
        @floatToInt(u64, @divTrunc(@intToFloat(f64, total_good.get()), duration / 1000)),
        data_len,
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
    var conerr: bool = false;
    while(barrier.isRunning()) {
        info("barrier is running", .{});
        stream = std.net.tcpConnectToAddress(parsed_address) catch|err|{
            warn("unable to connect: {};\n", .{err});
            conerr = true;
            break;
        };
        break;
    }
    try send_data(&stream, rbarrier);    
    if(!conerr) {
        _ = stream.close();
    }
}

fn uring_setup() !void {

    var num_rings: usize = 0;
    var send_rings = ArrayList(std.os.linux.IO_Uring).init(allocator);

    while(num_rings < arg_count) : (num_rings+=1) {    
        _ = try send_rings.append(undefined);
    }

    std.debug.print("only send supported with uring currently.\n", .{});
    std.debug.print("waiting for {} threads to finish.\n", .{num_rings});

    var ring_index: usize = 0;
    errdefer for (send_rings.items) |*send_ring| send_ring.deinit();

    while (ring_index < num_rings) : (ring_index += 1) {
        var send_params = switch (ring_index) {
            0 => std.mem.zeroInit(std.os.linux.io_uring_params, .{
                .flags = 0,
                .sq_thread_cpu = @intCast(u32, ring_index),
                .sq_thread_idle = 1000,
            }),
            else => std.mem.zeroInit(std.os.linux.io_uring_params, .{
                .flags = std.os.linux.IORING_SETUP_ATTACH_WQ,
                .wq_fd = @intCast(u32, send_rings.items[0].fd),
                .sq_thread_cpu = @intCast(u32, ring_index),
                .sq_thread_idle = 1000,
            }),
        };
        send_rings.items[ring_index] = try std.os.linux.IO_Uring.init_params(4096, &send_params);
    }

    var num_threads: usize = 0;
    var send_threads = ArrayList(std.Thread).init(allocator);

    while(num_threads < arg_count) : (num_threads+=1) {    
        _ = try send_threads.append(undefined);
    }

    var thread_index: usize = 0;
    defer for (send_threads.items) |*send_thread| send_thread.join();

    var barrier = Barrier{};

    var start = std.time.milliTimestamp();

    while (thread_index < num_threads) : (thread_index += 1) {
        send_threads.items[thread_index] = try std.Thread.spawn(.{}, uring_connect, .{ &send_rings.items[thread_index], &barrier });
    }

    var stats = &(try std.Thread.spawn(.{}, uring_stats, .{&barrier}));
    stats.detach();

    barrier.start();

    if(arg_duration >= 1000) {
        std.os.nanosleep(arg_duration / 1000, (arg_duration % 1000) * std.time.ns_per_ms);
    } else {
        std.os.nanosleep(0, arg_duration * std.time.ns_per_ms);
    }

    var end = std.time.milliTimestamp();
    var duration: f64 = @intToFloat(f64, end - start);

    info("total sent {} {}\n", .{total_sent.get(), duration});
    std.debug.print("{} total packets sent in {}ms\n{} packets sent/s @ {} bytes\n", .{
        total_sent.get(),
        end - start,
        @floatToInt(u64, @divTrunc(@intToFloat(f64, total_sent.get()), duration / 1000)),
        data_len,
    });
}

fn uring_connect(send_ring: *std.os.linux.IO_Uring, noalias barrier: *Barrier) !void {

    const client = &(try std.x.net.tcp.Client.init(.ip, .{ .close_on_exec = true, .nonblocking = true }));
    defer client.deinit();

    try client.setNoDelay(true);

    client.connect(std.x.net.ip.Address.initIPv4(std.x.os.IPv4.localhost, arg_port)) catch |err| switch (err) {
        error.WouldBlock => try client.getError(),
        else => return err,
    };

    try uring_send(send_ring, client, barrier);
}

fn uring_stats(noalias barrier: *const Barrier) !void {

    var timer = try std.time.Timer.start();

    while(barrier.isRunning()) {
        if (timer.read() > 1 * std.time.ns_per_s) {
            std.log.info("sent {} packet(s) in the last second", .{sent_pps.swap(0, .Monotonic)});
            std.log.info("received {} packet(s) in the last second", .{recv_pps.swap(0, .Monotonic)});
            timer.reset();
        }
    }
}

fn uring_send(ring: *std.os.linux.IO_Uring, client: *std.x.net.tcp.Client, noalias barrier: *Barrier) !void {

    var cqes: [4096]std.os.linux.io_uring_cqe = undefined;

    var packets_sent: u64 = 0;
    outer: while(barrier.isRunning()) {
        var i: usize = 0;
        while(i < 2048) : ( i+= 1) {
            _ = ring.send(0, client.socket.fd, data[0..data_len], std.os.linux.MSG.NOSIGNAL) catch |err| switch (err) {
                    error.SubmissionQueueFull => break,
                    else => return err,
            };
        }

        _ = ring.submit() catch |err| switch (err) {
            error.CompletionQueueOvercommitted, error.SystemResources => {},
            else => return err,
        };

        const num_completions = try ring.copy_cqes(&cqes, 0);
        for (cqes[0..num_completions]) |completion| {
            if (completion.res < 0) {
                return switch (std.os.errno(@intCast(usize, -completion.res))) {
                    .CONNREFUSED => error.ConnectionRefused,
                    .ACCES => error.AccessDenied,
                    .AGAIN => error.WouldBlock,
                    .ALREADY => error.FastOpenAlreadyInProgress,
                    .BADF => unreachable,
                    .CONNRESET => error.ConnectionResetByPeer,
                    .DESTADDRREQ => unreachable,
                    .FAULT => unreachable,
                    .INTR => continue,
                    .INVAL => unreachable,
                    .ISCONN => unreachable,
                    .MSGSIZE => error.MessageTooBig,
                    .NOBUFS => error.SystemResources,
                    .NOMEM => error.SystemResources,
                    .NOTSOCK => unreachable,
                    .OPNOTSUPP => unreachable,
                    .PIPE => error.BrokenPipe,
                    .AFNOSUPPORT => error.AddressFamilyNotSupported,
                    .LOOP => error.SymLinkLoop,
                    .NAMETOOLONG => error.NameTooLong,
                    .NOENT => error.FileNotFound,
                    .NOTDIR => error.NotDir,
                    .HOSTUNREACH => error.NetworkUnreachable,
                    .NETUNREACH => error.NetworkUnreachable,
                    .NOTCONN => error.SocketNotConnected,
                    .NETDOWN => error.NetworkSubsystemFailed,
                    else => |err| std.os.unexpectedErrno(err),
                };
            }
            _ = sent_pps.fetchAdd(1, .Monotonic);
            _ = total_sent.incr();  
            packets_sent += 1;
        }
        if((arg_numpacket > 0) and (packets_sent >= arg_numpacket)) {
            info("exited number. {}", .{std.Thread.getCurrentId()});
            barrier.stop();
            break :outer;
        }
    }
}

fn send_data(net_stream: *const std.net.Stream, noalias rbarrier: *const Barrier) anyerror!void {
    
    var sent: u64 = 0;
    var loop: u64 = 0;
    var serr: u64 = 0;

    var match = &(try std.Thread.spawn(.{}, matcher, .{net_stream, rbarrier}));

    var start = std.time.milliTimestamp();
    while (rbarrier.isRunning()) {

        var to_send = data;
        if(net_stream.write(to_send[0..data_len])) |err| {
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

        if((arg_numpacket > 0) and (loop >= arg_numpacket)) {
            info("exited number. {}", .{std.Thread.getCurrentId()});
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
    const buf = try allocator.alloc(u8, data_len);
    while (rbarrier.isRunning()) {
        var bytes = net_stream.read(buf[0..]) catch break;

        if(bytes == -1) {
            break;
        } else {
            recv += 1;
            _ = total_recv.incr();
        }

        if(std.mem.eql(u8, buf[0..], data[0..data_len])) {
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