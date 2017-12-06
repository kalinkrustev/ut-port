const hrtime = require('browser-process-hrtime');
const Buffer = require('buffer').Buffer;
const bufferCreate = Buffer;
const pull = require('pull-stream');
const pullStream = require('stream-to-pull-stream');
const paramap = require('pull-paramap');
const DISCARD = Symbol('discard packet');

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

const packetTimer = (method, aggregate = '*', timeout) => {
    if (!method) {
        return;
    }
    let time = hrtime();
    let times = {
        method,
        aggregate,
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
        what && (times[what] = (newtime[0] - time[0]) * 1000 + (newtime[1] - time[1]) / 1000000);
        time = newtime;
        if (what) {
            let isTimeout = Array.isArray(timeout) && ((newtime[0] - timeout[0]) * 1000 + (newtime[1] - timeout[1]) / 1000000 > 0);
            if (isTimeout) {
                timeout = false;
            }
            return isTimeout;
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
        times.aggregate && port.methodLatency(times.aggregate, {m: times.aggregate}, [
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

const traceMeta = ($meta, context) => {
    if ($meta && !$meta.timer) {
        $meta.timer = packetTimer($meta.method, '*', $meta.timeout);
    }
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
            traceMeta($meta, context);
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

const portDecode = (port, context, buffer) => decodePacket => {
    port.log.trace && port.log.trace({$meta: {mtid: 'frame', opcode: 'in'}, message: decodePacket, log: context && context.session && context.session.log});
    if (port.framePattern) {
        port.bytesReceived && port.bytesReceived(decodePacket.length);
        // todo check buffer size
        buffer = Buffer.concat([buffer, decodePacket]);
        let frame = getFrame(port, buffer);
        if (frame) {
            buffer = frame.rest;
            decodePacket = frame.data;
        } else {
            return Promise.resolve([DISCARD]);
        }
    }
    port.msgReceived && port.msgReceived(1);
    if (port.codec) {
        let $meta = {conId: context && context.conId};
        return Promise.resolve()
            .then(function decodeCall() {
                return port.codec.decode(decodePacket, $meta, context);
            })
            .then(decodedPacket => [decodedPacket, traceMeta($meta, context)])
            .catch(decodeError => {
                $meta.mtid = 'error';
                if (!decodeError || !decodeError.keepConnection) {
                    return [port.errors.disconnect(decodeError), $meta];
                } else {
                    return [decodeError, $meta];
                }
            });
    } else if (decodePacket && decodePacket.constructor && decodePacket.constructor.name === 'Buffer') {
        return Promise.resolve([{payload: decodePacket}, {mtid: 'notification', opcode: 'payload', conId: context && context.conId}]);
    } else {
        let $meta = (decodePacket.length > 1) && decodePacket[decodePacket.length - 1];
        $meta && context && context.conId && ($meta.conId = context.conId);
        (decodePacket.length > 1) && (decodePacket[decodePacket.length - 1] = traceMeta($meta, context));
        return Promise.resolve(decodePacket);
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
    if (event === 'disconnected' && context.requests.size) {
        let requests = context.requests.values();
        var request = requests.next();
        while (request && !request.done) {
            request.value.$meta.mtid = 'error';
            request.value.$meta.dispatch && request.value.$meta.dispatch(port.errors.disconnectBeforeResponse(), request.value.$meta);
            request = requests.next();
        }
        context.requests.clear();
    }
    return [message, {
        mtid: 'event',
        method: event,
        conId: context && context.conId,
        timer: packetTimer('event.' + event, false)
    }];
};

const portEventDispatch = (port, context, message, event, logger, queue) => pull(
    pull.once(portQueueEventCreate(port, context, message, event, logger)),
    paraPromise(port, portReceive(port, context), port.activeReceiveCount), calcTime(port, 'receive'),
    paraPromise(port, portDispatch(port), port.activeDispatchCount), calcTime(port, 'dispatch'),
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
    queue && queue.push(sinkPacket);
}, sinkError => {
    try {
        sinkError && port.error(sinkError);
    } finally {
        sinkError && queue && queue.end(sinkError);
    }
});

const paraPromise = (port, fn, counter, concurrency = 1) => {
    let active = 0;
    counter && counter(active);
    return paramap((data, cb) => {
        active++;
        counter && counter(active);
        Promise.resolve(data)
            .then(fn)
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
        callback: () => {
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

const portPull = (port, what, context) => {
    let stream;
    let result;
    context && (context.requests = new Map());
    let sendQueue = port.sendQueues.create({context});
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
                $meta.timer = packetTimer($meta.method, '*', $meta.timeout);
                receiveQueue.push(pushPacket);
            }
        };
    } else if (typeof what === 'function') {
        stream = pull(
            paraPromise(port, portExec(port, what), port.activeExecCount, port.config.concurrency || 10),
            calcTime(port, 'exec')
        );
    } else if (what.readable && what.writable) {
        stream = portDuplex(port, context, what);
    }
    let send = paraPromise(port, portSend(port, context), port.activeSendCount, port.config.concurrency || 10);
    let encode = paraPromise(port, portEncode(port, context), port.activeEncodeCount, port.config.concurrency || 10);
    let unpack = portUnpack(port);
    let idleSend = portIdleSend(port, context, sendQueue);
    let decode = paraPromise(port, portDecode(port, context, bufferCreate(0)), port.activeDecodeCount, port.config.concurrency || 10);
    let idleReceive = portIdleReceive(port, context, sendQueue);
    let receive = paraPromise(port, portReceive(port, context), port.activeReceiveCount, port.config.concurrency || 10);
    let dispatch = paraPromise(port, portDispatch(port), port.activeDispatchCount, port.config.concurrency || 10);
    let sink = portSink(port, sendQueue);
    pull(
        sendQueue, calcTime(port, 'queue', portTimeoutDispatch(port)),
        send, calcTime(port, 'send', portTimeoutDispatch(port)),
        encode, calcTime(port, 'encode', portTimeoutDispatch(port)),
        unpack,
        idleSend,
        stream,
        decode, calcTime(port, 'decode', portTimeoutDispatch(port, sendQueue)),
        idleReceive,
        receive, calcTime(port, 'receive', portTimeoutDispatch(port, sendQueue)),
        dispatch, calcTime(port, 'dispatch'),
        sink);
    portEventDispatch(port, context, DISCARD, 'connected', port.log.info, sendQueue);
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
    $meta.timer = packetTimer($meta.method, '*', $meta.timeout);
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
    portPush
};
