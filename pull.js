const hrtime = require('browser-process-hrtime');
const Buffer = require('buffer').Buffer;
const bufferCreate = Buffer;
const pull = require('pull-stream');
const pullStream = require('stream-to-pull-stream');
const errors = require('./errors');
const paramap = require('pull-paramap');
const DISCARD = Symbol('discard packet');

const portErrorDispatch = (port, $meta) => err => {
    port.error(err);
    $meta.mtid = 'error';
    $meta.errorCode = err && err.code;
    $meta.errorMessage = err && err.message;
    return portDispatch(port)([err, $meta]).then(() => [DISCARD, $meta]);
};

function packetTimer(method) {
    if (!method) {
        return;
    }
    let time = hrtime();
    let result = {
        method,
        queue: null,
        receive: null,
        encode: null,
        exec: null,
        decode: null,
        send: null,
        dispatch: null
    };

    return what => {
        let newtime = hrtime();
        what && (result[what] = (newtime[0] - time[0]) * 1000 + (newtime[1] - time[1]) / 1000000);
        time = newtime;
        return result;
    };
}

const calcTime = stage => pull(
    pull.filter(packet => {
        let $meta = (packet.length && packet[packet.length - 1]);
        $meta && $meta.timer && $meta.timer(stage);
        return (packet && packet[0] !== DISCARD);
    }),
    pull.map(packet => {
        if (packet && packet[0] instanceof errors.disconnect) {
            throw packet[0];
        } else {
            return packet;
        }
    })
);

const reportTimes = (port, $meta) => {
    if ($meta && $meta.timer && port.methodLatency) {
        let times = $meta.timer();
        port.methodLatency(times.method, {m: times.method}, [
            times.queue,
            times.receive,
            times.encode,
            times.exec,
            times.decode,
            times.send,
            times.dispatch
        ], 1);
        port.methodLatency('*', {m: '*'}, [
            times.queue,
            times.receive,
            times.encode,
            times.exec,
            times.decode,
            times.send,
            times.dispatch
        ], 1);
        delete $meta.timer;
    }
};

function traceMeta($meta, context) {
    if ($meta && $meta.trace && context) {
        if ($meta.mtid === 'request') { // todo improve what needs to be tracked
            let expireTimeout = 60000;
            context.requests.set($meta.trace, {
                $meta: $meta, expire: Date.now() + expireTimeout, startTime: hrtime()
            });
            return $meta;
        } else if ($meta.mtid === 'response' || $meta.mtid === 'error') {
            let x = context.requests.get($meta.trace);
            if (x) {
                context.requests.delete($meta.trace);
                return Object.assign(x.$meta, $meta);
            } else {
                return $meta;
            }
        }
    } else {
        return $meta;
    }
};

const portSend = (port, context) => packet => {
    let $meta = packet.length && packet[packet.length - 1];
    let {fn, name} = port.getConversion($meta, 'send');
    if (fn) {
        return Promise.resolve()
            .then(() => fn.apply(port, Array.prototype.concat(packet, context)))
            .then(result => {
                port.log.debug && port.log.debug({message: result, $meta: {method: name, mtid: 'convert'}});
                packet[0] = result;
                return packet;
            })
            .catch(portErrorDispatch(port, $meta));
    } else {
        return Promise.resolve(packet);
    }
};

const portEncode = (port, context) => packet => {
    let $meta = packet.length && packet[packet.length - 1];
    port.log.debug && port.log.debug({message: packet[0], $meta});
    return Promise.resolve()
        .then(() => port.codec ? port.codec.encode(packet[0], $meta, context) : packet)
        .then(buffer => {
            let size;
            let sizeAdjust = 0;
            traceMeta($meta, context);
            if (port.codec) {
                if (port.framePatternSize) {
                    sizeAdjust = port.config.format.sizeAdjust;
                }
                size = buffer && buffer.length + sizeAdjust;
            } else {
                size = buffer && buffer.length;
            }
            if (port.frameBuilder) {
                buffer = port.frameBuilder({size: size, data: buffer});
                buffer = buffer.slice(0, buffer.length - sizeAdjust);
                port.bytesSent && port.bytesSent(buffer.length);
            }
            if (buffer) {
                port.msgSent && port.msgSent(1);
                port.log.trace && port.log.trace({$meta: {mtid: 'frame', opcode: 'out'}, message: buffer});
                return port.frameBuilder ? [buffer, $meta] : buffer;
            }
            return [DISCARD, $meta];
        })
        .catch(portErrorDispatch(port, $meta));
};

const portExec = (port, fn) => packet => {
    let $meta = packet.length > 1 && packet[packet.length - 1];
    if ($meta && $meta.mtid === 'request') {
        $meta.mtid = 'response';
    }
    return Promise.resolve()
        .then(() => fn.apply(port, packet))
        .then(result => [result, $meta])
        .catch(error => {
            port.error(error);
            if ($meta) {
                $meta.mtid = 'error';
            }
            return [error, $meta];
        });
};

function getFrame(port, buffer) {
    if (port.framePatternSize) {
        let tmp = port.framePatternSize(buffer);
        if (tmp) {
            return port.framePattern(tmp.data, {size: tmp.size - port.config.format.sizeAdjust});
        } else {
            return false;
        }
    } else {
        return port.framePattern(buffer);
    }
}

const portDecode = (port, context, buffer) => packet => {
    port.log.trace && port.log.trace({$meta: {mtid: 'frame', opcode: 'in'}, message: packet});
    if (port.framePattern) {
        port.bytesReceived && port.bytesReceived(packet.length);
        buffer = Buffer.concat([buffer, packet]);
        let frame = getFrame(port, buffer);
        if (frame) {
            buffer = frame.rest;
            packet = frame.data;
        } else {
            return Promise.resolve([DISCARD]);
        }
    }
    port.msgReceived && port.msgReceived(1);
    if (port.codec) {
        let $meta = {conId: context && context.conId};
        return Promise.resolve()
            .then(() => port.codec.decode(packet, $meta, context))
            .then(decoded => [decoded, traceMeta($meta, context)])
            .catch(error => {
                $meta.mtid = 'error';
                if (!error || !error.keepConnection) {
                    return [errors.disconnect(error), $meta];
                } else {
                    return [error, $meta];
                }
            });
    } else if (packet && packet.constructor && packet.constructor.name === 'Buffer') {
        return Promise.resolve([{payload: packet}, {mtid: 'notification', opcode: 'payload', conId: context && context.conId}]);
    } else {
        let $meta = (packet.length > 1) && packet[packet.length - 1];
        $meta && context && context.conId && ($meta.conId = context.conId);
        (packet.length > 1) && (packet[packet.length - 1] = traceMeta($meta, context));
        return Promise.resolve(packet);
    }
};

const portReceive = (port, context) => packet => {
    let $meta = packet.length && packet[packet.length - 1];
    let {fn, name} = port.getConversion($meta, 'receive');
    if (!fn) {
        return Promise.resolve(packet);
    } else {
        return Promise.resolve()
            .then(() => fn.apply(port, Array.prototype.concat(packet, context)))
            .then(received => {
                port.log.debug && port.log.debug({message: received, $meta: {method: name, mtid: 'convert'}});
                return [received, $meta];
            })
            .catch(error => {
                port.error(error);
                $meta.mtid = 'error';
                $meta.errorCode = error && error.code;
                $meta.errorMessage = error && error.message;
                return [error, $meta];
            });
    }
};

const portQueueEventCreate = (port, context, name) => {
    context && port.log.info && port.log.info({$meta: {mtid: 'event', opcode: 'port.' + name}, connection: context});
    return [false, {
        mtid: 'event',
        opcode: name,
        conId: context && context.conId,
        timer: packetTimer('event.' + name)
    }];
};

const portEventDispatch = (port, context, event, cb) => pull(
    pull.once(portQueueEventCreate(port, context, event)),
    paraPromise(port, portReceive(port, context), port.activeReceiveCount), calcTime('receive'),
    paraPromise(port, portDispatch(port), port.activeDispatchCount), calcTime('dispatch'),
    pull.drain(null, error => {
        error && port.error(error);
        cb && cb(error);
    })
);

const portDispatch = port => packet => {
    let $meta = (packet.length && packet[packet.length - 1]) || {};
    if ($meta && $meta.dispatch) {
        return Promise.resolve($meta.dispatch.apply(port, packet));
    }
    if (!packet || !packet[0] || packet[0] === DISCARD) {
        return Promise.resolve([DISCARD]);
    }
    let mtid = $meta.mtid;
    let opcode = $meta.opcode;

    let portDispatchResult = isError => result => {
        if (mtid === 'request' && $meta.mtid !== 'discard') {
            !$meta.mtid && ($meta.mtid = isError ? 'error' : 'response');
            !$meta.opcode && ($meta.opcode = opcode);
            isError && port.error(result);
            return [result, $meta];
        } else {
            return [DISCARD];
        }
    };

    return Promise.resolve()
        .then(() => port.messageDispatch.apply(port, packet))
        .then(portDispatchResult(false), portDispatchResult(true));
};

const portSink = queue => pull.drain(msg => {
    queue.push(msg);
}, end => {

});

const paraPromise = (port, fn, counter, concurrency = 1) => {
    let active = 0;
    counter && counter(active);
    return paramap((data, cb) => {
        active++;
        counter && counter(active);
        fn(data)
            .then(result => {
                active--;
                counter && counter(active);
                cb(null, result);
                return true;
            }, error => {
                active--;
                counter && counter(active);
                cb(error);
            })
            .catch(error => {
                port.error(error);
                cb(error);
            });
    }, concurrency, false);
};

const portPull = (port, what, context) => {
    let stream;
    let result;
    context && (context.requests = new Map());
    if (!what) {
        let receiveQueue = require('pull-pushable')(true);
        stream = {
            sink: pull.drain(packet => {
                let $meta = packet.length > 1 && packet[packet.length - 1];
                reportTimes(port, $meta);
                if ($meta && $meta.reply) {
                    $meta.reply.apply(null, packet);
                }
            }, () => portEventDispatch(port, context, 'disconnected')),
            source: receiveQueue.source
        };
        result = {
            push: packet => {
                let $meta = (packet.length && packet[packet.length - 1]);
                $meta.timer = packetTimer(port.methodPath($meta.method) || $meta.method);
                receiveQueue.push(packet);
            }
        };
    } else if (typeof what === 'function') {
        stream = pull(
            paraPromise(port, portExec(port, what), port.activeExecCount, port.config.concurrency || 10),
            calcTime('exec')
        );
    } else if (what.readable && what.writable) {
        what.on('close', () => portEventDispatch(port, context, 'disconnected'));
        what.on('error', error => {
            port.error(errors.stream({context, error}));
        });
        port.socketTimeOut && what.setTimeout(port.socketTimeOut, () => {
            what.end();
        });
        stream = pullStream.duplex(what);
    }
    let buffer = bufferCreate(0);
    let sendQueue = port.portQueues.create({context});
    let send = paraPromise(port, portSend(port, context), port.activeSendCount, port.config.concurrency || 10);
    let encode = paraPromise(port, portEncode(port, context), port.activeEncodeCount, port.config.concurrency || 10);
    let unpack = pull.map(packet => port.frameBuilder ? packet[0] : packet);
    let decode = paraPromise(port, portDecode(port, context, buffer), port.activeDecodeCount, port.config.concurrency || 10);
    let receive = paraPromise(port, portReceive(port, context), port.activeReceiveCount, port.config.concurrency || 10);
    let dispatch = paraPromise(port, portDispatch(port), port.activeDispatchCount, port.config.concurrency || 10);
    let sink = portSink(sendQueue);
    pull(
        sendQueue, calcTime('queue'),
        send, calcTime('send'),
        encode, calcTime('encode'),
        unpack,
        stream,
        decode, calcTime('decode'),
        receive, calcTime('receive'),
        dispatch, calcTime('dispatch'),
        sink);
    portEventDispatch(port, context, 'connected', error => error && sink.abort(error));
    return result;
};

const portFindRoute = (port, $meta, args) => port.portQueues.get() ||
    port.portQueues.get($meta) ||
    (typeof port.connRouter === 'function' && port.portQueues.get({conId: port.connRouter(port.portQueues, args)}));

const portPush = (port, promise, args) => {
    if (!args.length) {
        return Promise.reject(errors.missingParams());
    } else if (args.length === 1 || !args[args.length - 1]) {
        return Promise.reject(errors.missingMeta());
    }
    let $meta = args[args.length - 1];
    let queue = portFindRoute(port, $meta, args);
    if (!queue) {
        port.log.error && port.log.error('Queue not found', {arguments: args});
        return promise ? Promise.reject(errors.notConnected(port.config.id)) : false;
    }
    if (!promise) {
        queue.push(args);
        return true;
    }
    return new Promise(function requestPromise(resolve, reject) {
        $meta.dispatch = function requestPromiseCb(msg) {
            reportTimes(port, $meta);
            if ($meta.mtid !== 'error') {
                resolve(Array.prototype.slice.call(arguments));
            } else {
                reject(msg);
            }
            return [DISCARD];
        };
        $meta.timer = packetTimer(port.methodPath($meta.method) || $meta.method);
        queue.push(args);
    });
};

module.exports = {
    portPull,
    portPush
};
