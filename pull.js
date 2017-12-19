const timing = require('./timing');
const Buffer = require('buffer').Buffer;
const bufferCreate = Buffer;
const pull = require('pull-stream');
const pullStream = require('stream-to-pull-stream');
const paramap = require('pull-paramap');
const DISCARD = Symbol('ut-port.pull.DISCARD');
const CONNECTED = Symbol('ut-port.pull.CONNECTED');

const portErrorDispatch = (port, $meta) => dispatchError => {
    port.error(dispatchError);
    $meta.mtid = 'error';
    $meta.errorCode = dispatchError && dispatchError.code;
    $meta.errorMessage = dispatchError && dispatchError.message;
    return portDispatch(port)([dispatchError, $meta]).then(() => [DISCARD, $meta]);
};

const portTimeoutDispatch = (port, sendQueue) => $meta => {
    if (sendQueue && !$meta.dispatch && $meta.mtid === 'request') {
        $meta.dispatch = (...packet) => {
            delete $meta.dispatch;
            sendQueue.push(packet);
            return [DISCARD];
        };
    }
    return portErrorDispatch(port, $meta)(port.errors.timeout()).catch(error => {
        port.error(error);
    });
};

const packetTimer = (method, aggregate = '*', id, timeout) => {
    if (!method) {
        return;
    }
    let time = timing.now();
    let times = {
        port: id,
        method,
        aggregate,
        queue: undefined,
        receive: undefined,
        encode: undefined,
        exec: undefined,
        decode: undefined,
        send: undefined,
        dispatch: undefined,
        calls: undefined
    };

    return (what, newtime = timing.now()) => {
        if (typeof what === 'object') {
            times.calls = times.calls || [];
            times.calls.push(what);
            return;
        }
        what && (times[what] = timing.diff(time, newtime));
        time = newtime;
        if (what) {
            if (timing.isAfter(newtime, timeout)) {
                timeout = false;
                return true;
            } else {
                return false;
            }
        }
        return times;
    };
};

const calcTime = (port, stage, onTimeout) => pull(
    pull.filter(packetFilter => {
        let $meta = packetFilter && packetFilter.length > 1 && packetFilter[packetFilter.length - 1];
        if ($meta && $meta.timer && $meta.timer(stage)) {
            onTimeout && onTimeout($meta);
            return false;
        };
        return (packetFilter && packetFilter[0] !== DISCARD);
    }),
    pull.map(packetThrow => {
        if (packetThrow && (packetThrow[0] instanceof port.errors.disconnect || packetThrow[0] instanceof port.errors.receiveTimeout)) {
            throw packetThrow[0];
        } else {
            return packetThrow;
        }
    })
);

const reportTimes = (port, $meta) => {
    if ($meta && $meta.timer && port.methodLatency && $meta.mtid !== 'request') {
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
        times.aggregate && port.methodLatency(times.aggregate, {m: times.aggregate}, [
            times.queue,
            times.receive,
            times.encode,
            times.exec,
            times.decode,
            times.send,
            times.dispatch
        ], 1);
        delete times.aggregate;
        $meta.calls = times;
        delete $meta.timer;
    }
};

const traceMeta = ($meta, context, id, set, get, time) => {
    if ($meta && !$meta.timer && $meta.mtid === 'request') {
        $meta.timer = packetTimer($meta.method, '*', id, $meta.timeout);
    }
    if ($meta && $meta.trace && context) {
        if ($meta.mtid === 'request') { // todo improve what needs to be tracked
            context.requests.set(set + $meta.trace, {$meta});
            return $meta;
        } else if ($meta.mtid === 'response' || $meta.mtid === 'error') {
            let request = context.requests.get(get + $meta.trace);
            if (request) {
                context.requests.delete(get + $meta.trace);
                request.$meta && request.$meta.timer && time && request.$meta.timer('exec', time);
                return Object.assign(request.$meta, $meta);
            } else {
                return $meta;
            }
        }
    } else {
        return $meta;
    }
};

const portSend = (port, context) => sendPacket => {
    let $meta = sendPacket.length > 1 && sendPacket[sendPacket.length - 1];
    let {fn, name} = port.getConversion($meta, 'send');
    if (fn) {
        return Promise.resolve()
            .then(function sendCall() {
                return fn.apply(port, Array.prototype.concat(sendPacket, context));
            })
            .then(result => {
                port.log.trace && port.log.trace({message: result, $meta: {method: name, mtid: 'convert'}, log: context && context.session && context.session.log});
                sendPacket[0] = result;
                return sendPacket;
            })
            .catch(portErrorDispatch(port, $meta));
    } else {
        return Promise.resolve(sendPacket);
    }
};

const portEncode = (port, context) => encodePacket => {
    let $meta = encodePacket.length > 1 && encodePacket[encodePacket.length - 1];
    port.log.debug && port.log.debug({message: encodePacket[0], $meta, log: context && context.session && context.session.log});
    return Promise.resolve()
        .then(function encodeCall() {
            return port.codec ? port.codec.encode(encodePacket[0], $meta, context) : encodePacket;
        })
        .then(encodeBuffer => {
            let size;
            let sizeAdjust = 0;
            traceMeta($meta, context, port.config.id, 'out/', 'in/');
            if (port.codec) {
                if (port.framePatternSize) {
                    sizeAdjust = port.config.format.sizeAdjust;
                }
                size = encodeBuffer && encodeBuffer.length + sizeAdjust;
            } else {
                size = encodeBuffer && encodeBuffer.length;
            }
            if (port.frameBuilder) {
                encodeBuffer = port.frameBuilder({size: size, data: encodeBuffer});
                encodeBuffer = encodeBuffer.slice(0, encodeBuffer.length - sizeAdjust);
                port.bytesSent && port.bytesSent(encodeBuffer.length);
            }
            if (encodeBuffer) {
                port.msgSent && port.msgSent(1);
                port.log.trace && port.log.trace({$meta: {mtid: 'frame', opcode: 'out'}, message: encodeBuffer, log: context && context.session && context.session.log});
                return port.frameBuilder ? [encodeBuffer, $meta] : encodeBuffer;
            }
            return [DISCARD, $meta];
        })
        .catch(portErrorDispatch(port, $meta));
};

const portUnpack = port => pull.map(unpackPacket => port.frameBuilder ? unpackPacket[0] : unpackPacket);

const portIdleSend = (port, context, queue) => {
    let timer;
    if (port.config.idleSend) {
        let idleSendReset = () => {
            timer && clearTimeout(timer);
            timer = setTimeout(() => {
                portEventDispatch(port, context, DISCARD, 'idleSend', port.log.trace, queue);
                idleSendReset();
            }, port.config.idleSend);
        };
        idleSendReset();
        return pull.through(packet => {
            idleSendReset();
            return packet;
        }, () => {
            timer && clearTimeout(timer);
            timer = null;
        });
    }
};

const portExec = (port, fn) => execPacket => {
    let $meta = execPacket.length > 1 && execPacket[execPacket.length - 1];
    if ($meta && $meta.mtid === 'request') {
        $meta.mtid = 'response';
    }
    return Promise.resolve()
        .then(function execCall() {
            return fn.apply(port, execPacket);
        })
        .then(execResult => [execResult, $meta])
        .catch(execError => {
            port.error(execError);
            if ($meta) {
                $meta.mtid = 'error';
            }
            return [execError, $meta];
        });
};

const getFrame = (port, buffer) => {
    let result;
    let size;
    if (port.framePatternSize) {
        let tmp = port.framePatternSize(buffer);
        if (tmp) {
            size = tmp.size;
            result = port.framePattern(tmp.data, {size: tmp.size - port.config.format.sizeAdjust});
        } else {
            result = false;
        }
    } else {
        result = port.framePattern(buffer);
    }
    if (port.config.maxReceiveBuffer) {
        if (!result && buffer.length > port.config.maxReceiveBuffer) {
            throw port.errors.bufferOverflow({params: {max: port.config.maxReceiveBuffer, size: buffer.length}});
        }
        if (!result && size > port.config.maxReceiveBuffer) { // fail early
            throw port.errors.bufferOverflow({params: {max: port.config.maxReceiveBuffer, size: size}});
        }
    }
    return result;
};

const portUnframe = (port, context, buffer) => {
    return port.framePattern && pull(
        pull.map(datagram => {
            let result = [];
            port.bytesReceived && port.bytesReceived(datagram.length);
            port.log.trace && port.log.trace({$meta: {mtid: 'frame', opcode: 'in'}, message: datagram, log: context && context.session && context.session.log});
            // todo check buffer size
            buffer = Buffer.concat([buffer, datagram]);
            let dataPacket;
            while ((dataPacket = getFrame(port, buffer))) {
                buffer = dataPacket.rest;
                result.push(dataPacket.data);
            }
            return result;
        }),
        pull.flatten()
    );
};

const portDecode = (port, context) => dataPacket => {
    let time = timing.now();
    port.msgReceived && port.msgReceived(1);
    if (port.codec) {
        let $meta = {conId: context && context.conId};
        return Promise.resolve()
            .then(function decodeCall() {
                return port.codec.decode(dataPacket, $meta, context);
            })
            .then(decodeResult => [decodeResult, traceMeta($meta, context, port.config.id, 'in/', 'out/', time)])
            .catch(decodeError => {
                $meta.mtid = 'error';
                if (!decodeError || !decodeError.keepConnection) {
                    return [port.errors.disconnect(decodeError), $meta];
                } else {
                    return [decodeError, $meta];
                }
            });
    } else if (dataPacket && dataPacket.constructor && dataPacket.constructor.name === 'Buffer') {
        return Promise.resolve([{payload: dataPacket}, {mtid: 'notification', opcode: 'payload', conId: context && context.conId}]);
    } else {
        let $meta = (dataPacket.length > 1) && dataPacket[dataPacket.length - 1];
        $meta && context && context.conId && ($meta.conId = context.conId);
        (dataPacket.length > 1) && (dataPacket[dataPacket.length - 1] = traceMeta($meta, context, 'in/', 'out/', time));
        return Promise.resolve(dataPacket);
    }
};

const portIdleReceive = (port, context, queue) => {
    let timer;
    if (port.config.idleReceive) {
        let idleReceiveReset = () => {
            timer && clearTimeout(timer);
            timer = setTimeout(() => {
                portEventDispatch(
                    port,
                    context,
                    port.errors.receiveTimeout({params: {timeout: port.config.idleReceive}}),
                    'idleReceive',
                    port.log.trace,
                    queue
                );
                idleReceiveReset();
            }, port.config.idleReceive);
        };
        idleReceiveReset();
        return pull.through(packet => {
            idleReceiveReset();
            return packet;
        }, () => {
            timer && clearTimeout(timer);
            timer = null;
        });
    }
};

const portReceive = (port, context) => receivePacket => {
    let $meta = receivePacket.length > 1 && receivePacket[receivePacket.length - 1];
    let {fn, name} = port.getConversion($meta, 'receive');
    if (!fn) {
        return Promise.resolve(receivePacket);
    } else {
        return Promise.resolve()
            .then(function receiveCall() {
                return fn.apply(port, Array.prototype.concat(receivePacket, context));
            })
            .then(receivedPacket => {
                port.log.trace && port.log.trace({message: receivedPacket, $meta: {method: name, mtid: 'convert'}, log: context && context.session && context.session.log});
                return [receivedPacket, $meta];
            })
            .catch(receiveError => {
                port.error(receiveError);
                $meta.mtid = 'error';
                $meta.errorCode = receiveError && receiveError.code;
                $meta.errorMessage = receiveError && receiveError.message;
                return [receiveError, $meta];
            });
    }
};

const portQueueEventCreate = (port, context, message, event, logger) => {
    context && (typeof logger === 'function') && logger({$meta: {mtid: 'event', opcode: 'port.' + event}, connection: context, log: context && context.session && context.session.log});
    if (event === 'disconnected') {
        if (context && context.requests && context.requests.size) {
            Array.from(context.requests.values()).forEach(request => {
                request.$meta.mtid = 'error';
                request.$meta.dispatch && request.$meta.dispatch(port.errors.disconnectBeforeResponse(), request.$meta);
            });
            context.requests.clear();
        }
        if (context && context.waiting && context.waiting.size) {
            Array.from(context.waiting.values()).forEach(end => {
                end(port.errors.disconnectBeforeResponse());
            });
        }
    }
    return [message, {
        mtid: 'event',
        method: event,
        conId: context && context.conId,
        timer: packetTimer('event.' + event, false, port.config.id)
    }];
};

const portEventDispatch = (port, context, message, event, logger, queue) => pull(
    pull.once(portQueueEventCreate(port, context, message, event, logger)),
    paraPromise(port, context, portReceive(port, context), port.activeReceiveCount), calcTime(port, 'receive'),
    paraPromise(port, context, portDispatch(port), port.activeDispatchCount), calcTime(port, 'dispatch'),
    portSink(port, queue)
);

const portDispatch = port => dispatchPacket => {
    let $meta = (dispatchPacket.length > 1 && dispatchPacket[dispatchPacket.length - 1]) || {};
    if ($meta && $meta.dispatch) {
        reportTimes(port, $meta);
        return Promise.resolve().then(() => $meta.dispatch.apply(port, dispatchPacket));
    }
    if (!dispatchPacket || !dispatchPacket[0] || dispatchPacket[0] === DISCARD) {
        return Promise.resolve([DISCARD]);
    }
    if (dispatchPacket[0] === CONNECTED) {
        return Promise.resolve([CONNECTED]);
    }
    let mtid = $meta.mtid;
    let opcode = $meta.opcode;

    let portDispatchResult = isError => dispatchResult => {
        let $metaResult = (dispatchResult.length > 1 && dispatchResult[dispatchResult.length - 1]) || {};
        if (mtid === 'request' && $metaResult.mtid !== 'discard') {
            !$metaResult.opcode && ($metaResult.opcode = opcode);
            $metaResult.mtid = isError ? 'error' : 'response';
            $metaResult.reply = $meta.reply;
            $metaResult.timer = $meta.timer;
            $metaResult.dispatch = $meta.dispatch;
            $metaResult.trace = $meta.trace;
            if (isError) {
                port.error(dispatchResult);
                return [dispatchResult, $metaResult];
            } else {
                return dispatchResult;
            }
        } else {
            return [DISCARD];
        }
    };

    if (mtid === 'error') {
        return Promise.reject(port.errors.unhandled(dispatchPacket[0]));
    }

    return Promise.resolve()
        .then(() => port.messageDispatch.apply(port, dispatchPacket))
        .then(portDispatchResult(false), portDispatchResult(true));
};

const portSink = (port, queue) => pull.drain(sinkPacket => {
    if (sinkPacket && sinkPacket[0] === CONNECTED) {
        typeof queue.start === 'function' && queue.start();
    } else {
        queue && queue.push(sinkPacket);
    }
}, sinkError => {
    try {
        sinkError && port.error(sinkError);
    } finally {
        sinkError && queue && queue.end(sinkError);
    }
});

function PromiseTimeout() {
    this.calls = new Set();
    this.interval = false;
}

PromiseTimeout.prototype.clean = function promiseClean() {
    let now = timing.now();
    Array.from(this.calls).forEach(end => end.checkTimeout(now));
};

PromiseTimeout.prototype.startWait = function promiseStartWait(onTimeout, timeout, createTimeoutError, set) {
    this.interval = this.interval || setInterval(this.clean.bind(this), 500);
    let end = error => {
        this.endWait(end, set);
        error && onTimeout(error);
    };
    end.checkTimeout = time => timing.isAfter(time, timeout) && end(createTimeoutError());
    this.calls.add(end);
    set && set.add(end);
    return end;
};

PromiseTimeout.prototype.endWait = function promiseEndWait(end, set) {
    this.calls.delete(end);
    set && set.delete(end);
    if (this.calls.size <= 0 && this.interval) {
        clearInterval(this.interval);
        this.interval = false;
    }
};

PromiseTimeout.prototype.start = function promiseStart(params, fn, $meta, error, set) {
    if (Array.isArray($meta && $meta.timeout)) {
        return new Promise((resolve, reject) => {
            let endWait = this.startWait(error => {
                $meta.mtid = 'error';
                if ($meta.dispatch) {
                    $meta.dispatch(error, $meta);
                    resolve([DISCARD]);
                } else {
                    resolve([error, $meta]);
                }
            }, $meta.timeout, error, set);
            Promise.resolve(params).then(fn)
            .then(result => {
                endWait();
                resolve(result);
                return result;
            })
            .catch(error => {
                endWait();
                reject(error);
            });
        });
    } else {
        return Promise.resolve(params).then(fn);
    }
};

const promiseTimeout = new PromiseTimeout();

const paraPromise = (port, context, fn, counter, concurrency = 1) => {
    let active = 0;
    counter && counter(active);
    return paramap((params, cb) => {
        active++;
        counter && counter(active);
        let $meta = params.length > 1 && params[params.length - 1];
        promiseTimeout.start(params, fn, $meta, port.errors.timeout, context && context.waiting)
            .then(promiseResult => {
                active--;
                counter && counter(active);
                cb(null, promiseResult);
                return true;
            }, promiseError => {
                active--;
                counter && counter(active);
                cb(promiseError);
            })
            .catch(cbError => {
                port.error(cbError);
                cb(cbError);
            });
    }, concurrency, false);
};

const portDuplex = (port, context, stream) => {
    let cleanup = () => {
        stream.removeListener('data', streamData);
        stream.removeListener('close', streamClose);
        stream.removeListener('error', streamError);
    };
    let closed = false;
    let receiveQueue = port.receiveQueues.create({
        context,
        close: () => {
            !closed && stream.end();
        }
    });
    let streamData = data => {
        receiveQueue.push(data);
    };
    let streamClose = () => {
        closed = true;
        cleanup();
        try {
            portEventDispatch(port, context, DISCARD, 'disconnected', port.log.info);
        } finally {
            receiveQueue.end();
        }
    };
    let streamError = error => {
        port.error(port.errors.stream({context, error}));
    };
    stream.on('error', streamError);
    stream.on('close', streamClose);
    stream.on('data', streamData);
    port.config.socketTimeOut && stream.setTimeout(port.config.socketTimeOut, () => {
        stream.destroy(port.errors.socketTimeout({params: {timeout: port.config.socketTimeOut}}));
    });
    let sink = pullStream.sink(stream);
    let source = receiveQueue.source;
    return {
        sink,
        source
    };
};

const drainSend = (port, context) => queueLength => {
    return portEventDispatch(port, context, {length: queueLength, interval: port.config.drainSend}, 'drainSend', port.log.info);
};

const portPull = (port, what, context) => {
    let stream;
    let result;
    context && (context.requests = new Map());
    context && (context.waiting = new Set());
    let sendQueue = port.sendQueues.create({
        min: port.config.minSend,
        max: port.config.maxSend,
        drainInterval: port.config.drainSend,
        drain: (port.config.minSend >= 0 || port.config.drainSend) && drainSend(port, context),
        skipDrain: packet => packet && packet.length > 1 && (packet[packet.length - 1].echo || packet[packet.length - 1].drain === false),
        context
    });
    if (!what) {
        let receiveQueue = port.receiveQueues.create({context});
        stream = {
            sink: pull.drain(replyPacket => {
                let $meta = replyPacket.length > 1 && replyPacket[replyPacket.length - 1];
                reportTimes(port, $meta);
                if ($meta && $meta.reply) {
                    let fn = $meta.reply;
                    delete $meta.reply;
                    try {
                        fn.apply(null, replyPacket);
                    } catch (error) {
                        port.error(error);
                    }
                }
            }, () => portEventDispatch(port, context, DISCARD, 'disconnected', port.log.info)),
            source: receiveQueue.source
        };
        result = {
            push: pushPacket => {
                let $meta = (pushPacket.length > 1 && pushPacket[pushPacket.length - 1]);
                $meta.method = $meta && $meta.method && $meta.method.split('/').pop();
                $meta.timer = $meta.timer || packetTimer($meta.method, '*', port.config.id, $meta.timeout);
                receiveQueue.push(pushPacket);
            }
        };
    } else if (typeof what === 'function') {
        stream = pull(
            paraPromise(port, context, portExec(port, what), port.activeExecCount, port.config.concurrency || 10),
            calcTime(port, 'exec', portTimeoutDispatch(port))
        );
    } else if (what.readable && what.writable) {
        stream = portDuplex(port, context, what);
    }
    let send = paraPromise(port, context, portSend(port, context), port.activeSendCount, port.config.concurrency || 10);
    let encode = paraPromise(port, context, portEncode(port, context), port.activeEncodeCount, port.config.concurrency || 10);
    let unpack = portUnpack(port);
    let idleSend = portIdleSend(port, context, sendQueue);
    let unframe = portUnframe(port, context, bufferCreate(0));
    let decode = paraPromise(port, context, portDecode(port, context), port.activeDecodeCount, port.config.concurrency || 10);
    let idleReceive = portIdleReceive(port, context, sendQueue);
    let receive = paraPromise(port, context, portReceive(port, context), port.activeReceiveCount, port.config.concurrency || 10);
    let dispatch = paraPromise(port, context, portDispatch(port), port.activeDispatchCount, port.config.concurrency || 10);
    let sink = portSink(port, sendQueue);
    pull(
        sendQueue, calcTime(port, 'queue', portTimeoutDispatch(port)),
        send, calcTime(port, 'send', portTimeoutDispatch(port)),
        encode, calcTime(port, 'encode', portTimeoutDispatch(port)),
        unpack,
        idleSend,
        stream,
        unframe,
        decode, calcTime(port, 'decode', portTimeoutDispatch(port, sendQueue)),
        idleReceive,
        receive, calcTime(port, 'receive', portTimeoutDispatch(port, sendQueue)),
        dispatch, calcTime(port, 'dispatch'),
        sink);
    portEventDispatch(port, context, CONNECTED, 'connected', port.log.info, sendQueue);
    return result;
};

const portFindRoute = (port, $meta, args) => port.sendQueues.get() ||
    port.sendQueues.get($meta) ||
    (typeof port.connRouter === 'function' && port.sendQueues.get({conId: port.connRouter(port.sendQueues, args)}));

const portPush = (port, promise, args) => {
    if (!args.length) {
        return Promise.reject(port.errors.missingParams());
    } else if (args.length === 1 || !args[args.length - 1]) {
        return Promise.reject(port.errors.missingMeta());
    }
    let $meta = args[args.length - 1] = Object.assign({}, args[args.length - 1]);
    let queue = portFindRoute(port, $meta, args);
    if (!queue) {
        port.log.error && port.log.error('Queue not found', {arguments: args});
        return promise ? Promise.reject(port.errors.notConnected()) : false;
    }
    $meta.method = $meta && $meta.method && $meta.method.split('/').pop();
    $meta.timer = packetTimer($meta.method, '*', port.config.id, $meta.timeout);
    if (!promise) {
        $meta.dispatch = () => {
            delete $meta.dispatch;
        }; // caller does not care for result;
        queue.push(args);
        return true;
    }
    return new Promise((resolve, reject) => {
        $meta.dispatch = (...params) => {
            delete $meta.dispatch;
            if ($meta.mtid !== 'error') {
                resolve(params);
            } else {
                reject(params[0]);
            }
            return [DISCARD];
        };
        queue.push(args);
    });
};

module.exports = {
    portPull,
    portPush,
    packetTimer
};
