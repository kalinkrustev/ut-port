'use strict';
const os = require('os');
const through2 = require('through2');
const Buffer = require('buffer').Buffer;
const bufferCreate = Buffer;
const hrtime = require('browser-process-hrtime');
const errors = require('./errors');
const includes = require('./includes');
const utqueue = require('ut-queue');
const pull = require('pull-stream');
const pullPromise = require('pull-promise');
const pullStream = require('stream-to-pull-stream');
const pullCat = require('pull-cat');
const UNLIMITED = Symbol('unlimited concurrency');
const DISCARD = Symbol('discard packet');

const handleStreamClose = (port, stream, conId, done) => () => {
    stream && stream.destroy();
    port.portQueues.delete({conId});
    (typeof done === 'function') && done();
};

function Port(params) {
    this.log = {};
    this.logFactory = (params && params.logFactory) || null;
    this.bus = (params && params.bus) || null;
    // this.portQueues = utqueue.queues();
    this.portQueues = utqueue.pushables();
    this.bytesSent = null;
    this.bytesReceived = null;
    this.counter = null;
    this.streams = [];
    // performance metric handlers
    this.msgSent = null;
    this.msgReceived = null;
    this.activeExecCount = null;
    this.activeSendCount = null;
    this.activeReceiveCount = null;
    this.isReady = false;
    this.state = 'stopped';
}

Port.prototype.init = function init() {
    this.logFactory && (this.log = this.logFactory.createLog(this.config.logLevel, {name: this.config.id, context: this.config.type + ' port'}, this.config.log));

    if (this.config.metrics !== false && this.bus && this.bus.config.implementation && this.bus.performance) {
        let measurementName = this.config.metrics || this.config.id;
        let tags = {
            host: os.hostname(),
            impl: this.bus.performance.config.id || this.bus.config.implementation
        };
        this.counter = function initCounters(fieldType, fieldCode, fieldName, interval) {
            return this.bus.performance.register(measurementName, fieldType, fieldCode, fieldName, 'standard', tags, interval);
        }.bind(this);
        this.latency = this.counter('average', 'lt', 'Latency', 300);
        this.msgSent = this.counter('counter', 'ms', 'Messages sent', 300);
        this.msgReceived = this.counter('counter', 'mr', 'Messages received', 300);
        this.activeExecCount = this.counter('gauge', 'ae', 'Active exec count', 300);
        this.activeSendCount = this.counter('gauge', 'as', 'Active send count');
        this.activeReceiveCount = this.counter('gauge', 'ar', 'Active receive count');
        if (this.bus.performance.measurements) {
            this.timeTaken = this.bus.performance.register(measurementName + '_tt', 'average', 'tt', 'Time taken', 'tagged', tags);
        }
    }

    let methods = {req: {}, pub: {}};
    methods.req[this.config.id + '.start'] = this.start;
    methods.req[this.config.id + '.stop'] = this.stop;

    (this.config.namespace || this.config.imports || [this.config.id]).reduce(function initReduceMethods(prev, next) {
        prev.req[next + '.request'] = this.request.bind(this);
        prev.pub[next + '.publish'] = this.publish.bind(this);
        return prev;
    }.bind(this), methods);

    return this.bus && Promise.all([
        this.bus.register(methods.req, 'ports'),
        this.bus.subscribe(methods.pub, 'ports'),
        this.bus && typeof this.bus.portEvent === 'function' && this.bus.portEvent('init', this)
    ]);
};

Port.prototype.messageDispatch = function messageDispatch() {
    let result = this.bus && this.bus.dispatch.apply(this.bus, Array.prototype.slice.call(arguments));
    if (!result) {
        this.log.error && this.log.error('Cannot dispatch message to bus', {message: Array.prototype.slice.call(arguments)});
    }
    return result;
};

Port.prototype.start = function start() {
    this.state = 'starting';
    return this.fireEvent('start', {config: this.config});
};

Port.prototype.ready = function ready() {
    return this.fireEvent('ready')
        .then((result) => {
            this.isReady = true;
            this.state = 'started';
            return result;
        });
};

Port.prototype.fireEvent = function fireEvent(event, logData) {
    this.log.info && this.log.info(Object.assign({
        $meta: {
            mtid: 'event',
            opcode: `port.${event}`
        }
    }, logData));

    let eventHandlers = this.config[event] ? [this.config[event]] : [];
    if (Array.isArray(this.config.imports) && this.config.imports.length) {
        let regExp = new RegExp(`\\.${event}$`);
        this.config.imports.forEach((imp) => {
            imp.match(regExp) && eventHandlers.push(this.config[imp]);
            this.config[`${imp}.${event}`] && eventHandlers.push(this.config[`${imp}.${event}`]);
        });
    }

    return eventHandlers.reduce((promise, eventHandler) => {
        promise = promise.then(() => eventHandler.call(this));
        return promise;
    }, Promise.resolve())
        .then(result =>
            Promise.resolve(this.bus && typeof this.bus.portEvent === 'function' && this.bus.portEvent(event, this)).then(() => result)
        );
};

Port.prototype.stop = function stop() {
    this.state = 'stopping';
    return this.fireEvent('stop')
        .then(() => {
            this.streams.forEach(function streamEnd(stream) {
                stream.end();
            });
            this.state = 'stopped';
            return true;
        });
};

const portFindRoute = (port, $meta, args) => port.portQueues.get() ||
    port.portQueues.get($meta) ||
    (typeof port.connRouter === 'function' && port.portQueues.get({conId: port.connRouter(port.portQueues, args)}));

Port.prototype.request = function request() {
    let args = Array.prototype.slice.call(arguments);
    if (!args.length) {
        return Promise.reject(errors.missingParams());
    } else if (args.length === 1 || !args[args.length - 1]) {
        return Promise.reject(errors.missingMeta());
    }
    let $meta = args[args.length - 1];
    let queue = portFindRoute(this, $meta, args);
    if (!queue) {
        this.log.error && this.log.error('Queue not found', {arguments: args});
        return Promise.reject(errors.notConnected(this.config.id));
    }
    return new Promise(function requestPromise(resolve, reject) {
        $meta.dispatch = function requestPromiseCb(msg) {
            if ($meta.mtid !== 'error') {
                resolve(Array.prototype.slice.call(arguments));
            } else {
                reject(msg);
            }
            return DISCARD;
        };
        $meta.startTime = hrtime();
        queue.push(args);
    });
};

Port.prototype.publish = function publish() {
    let args = Array.prototype.slice.call(arguments);
    if (!args.length) {
        return Promise.reject(errors.missingParams());
    } else if (args.length === 1 || !args[args.length - 1]) {
        return Promise.reject(errors.missingMeta());
    }
    let $meta = args[args.length - 1];
    let queue = portFindRoute(this, $meta, args);
    if (queue) {
        queue.push(args);
        return true;
    } else {
        this.log.error && this.log.error('Queue not found', {arguments: args});
        return false;
    }
};

Port.prototype.error = function portError(error) {
    this.log.error && this.log.error(error);
};

const portEncode = (port, context) => packet => {
    let $meta = packet.length && packet[packet.length - 1];
    port.log.debug && port.log.debug({message: packet[0], $meta});
    return Promise.resolve(port.codec ? port.codec.encode(packet[0], $meta, context) : packet)
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
                return buffer;
            }
            return DISCARD;
        });
};

function applyPattern(port, buffer) {
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
        let frame = applyPattern(port, buffer);
        if (frame) {
            buffer = frame.rest;
            packet = frame.data;
        } else {
            packet = DISCARD;
        }
    }
    if (port.codec) {
        let $meta = {conId: context && context.conId};
        return Promise.resolve(port.codec.decode(packet, $meta, context))
            .then(result => [result, $meta]);
    } else if (packet && packet.constructor && packet.constructor.name === 'Buffer') {
        return Promise.resolve([{payload: packet}, {mtid: 'notification', opcode: 'payload', conId: context && context.conId}]);
    } else {
        let $meta = (packet.length > 1) && packet[packet.length - 1];
        $meta && context && context.conId && ($meta.conId = context.conId);
        return Promise.resolve(packet);
    }
};

const portReceive = (port, context) => packet => {
    let $meta = packet.length && packet[packet.length - 1];
    $meta = traceMeta($meta, context);
    $meta.conId = context && context.conId;
    let {fn, name} = port.getConversion($meta, 'receive');
    $meta && (packet[packet.length - 1] = $meta);
    if (!fn) {
        return Promise.resolve(packet);
    } else {
        return Promise.resolve(fn.apply(port, Array.prototype.concat(packet, context)))
            .then(function receivePromiseResolved(result) {
                port.log.debug && port.log.debug({message: result, $meta: {method: name, mtid: 'convert'}});
                return [result, $meta];
            })
            .catch(function receivePromiseRejected(err) {
                port.error(err);
                $meta.mtid = 'error';
                $meta.errorCode = err && err.code;
                $meta.errorMessage = err && err.message;
                return [err, $meta];
            });
    }
};

Port.prototype.receive = function portPrototypeReceive(stream, packet, context) {
    return Promise.resolve(packet)
    .then(portReceive(this, context))
    .then(packet => stream.writable && stream.push(packet));
};

Port.prototype.decodeStream = function decode(context, concurrency) {
    let buffer = bufferCreate(0);
    return this.createStream(
        packet => Promise.resolve(packet)
            .then(portDecode(this, context, buffer))
            .then(portReceive(this, context)),
        concurrency,
        this.activeReceiveCount);
};

function traceMeta($meta, context) {
    if ($meta && $meta.trace && context) {
        if ($meta.dispatch && $meta.mtid === 'request') {
            let expireTimeout = 60000;
            context.requests[$meta.trace] = {
                $meta: $meta, expire: Date.now() + expireTimeout, startTime: Date.now()
            };
        } else if ($meta.mtid === 'response' || $meta.mtid === 'error') {
            let x = context.requests[$meta.trace];
            if (x) {
                delete context.requests[$meta.trace];
                if (x.startTime) {
                    $meta.timeTaken = Date.now() - x.startTime;
                }
                return Object.assign(x.$meta, $meta);
            } else {
                return $meta;
            }
        }
    } else {
        return $meta;
    }
};

Port.prototype.methodPath = function methodPath(methodName) {
    return methodName.split('/', 2)[1];
};

Port.prototype.getConversion = function getConversion($meta, type) {
    let fn;
    let name;
    if ($meta) {
        if ($meta.method) {
            name = [$meta.method, $meta.mtid, type].join('.');
            fn = this.config[name];
            if (!fn) {
                name = [this.methodPath($meta.method), $meta.mtid, type].join('.');
                fn = this.config[name];
            }
        }
        if (!fn) {
            name = [$meta.opcode, $meta.mtid, type].join('.');
            fn = this.config[name];
        }
    }
    if (!fn && (!$meta || $meta.mtid !== 'event')) {
        name = type;
        fn = this.config[name];
    }
    return {fn, name};
};

// concurrency can be a number (indicating the treshhold) or true (for unmilited concurrency)
Port.prototype.createStream = function createStream(handler, concurrency, activeCounter) {
    let countActive = 0;
    let port = this;
    if (!concurrency) {
        // set to 10 by default
        concurrency = this.config.concurrency || 10;
    }
    if (activeCounter) {
        activeCounter = activeCounter.bind(this);
    }
    let stream = through2({objectMode: true}, function createStreamThrough(packet, enc, callback) {
        countActive++;
        activeCounter && activeCounter(countActive);
        if (concurrency === UNLIMITED || (countActive < concurrency)) {
            callback();
        }
        return Promise.resolve()
            .then(function createStreamPromise() {
                return handler.call(stream, packet);
            })
            .catch(function createStreamPromiseCatch(e) {
                // TODO: handle error (e.g. close and recreate stream)
                port.error(e);
                stream.push(null);
                return DISCARD;
            })
            .then(function createStreamPromiseThen(res) {
                if (res !== DISCARD && stream.writable) {
                    stream.push(res);
                }
                countActive--;
                activeCounter && activeCounter(countActive);
                if (concurrency !== UNLIMITED && countActive + 1 >= concurrency) {
                    callback();
                }
                return res;
            });
    });
    return stream;
};

const portErrorCallback = (port, $meta) => err => {
    port.error(err);
    $meta.mtid = 'error';
    $meta.errorCode = err && err.code;
    $meta.errorMessage = err && err.message;
    $meta.callback && $meta.callback(err, $meta);
    return DISCARD;
};

const portSend = (port, context) => packet => {
    let $meta = packet.length && packet[packet.length - 1];
    let {fn, name} = port.getConversion($meta, 'send');
    if (fn) {
        return Promise.resolve(fn.apply(port, Array.prototype.concat(packet, context)))
            .then(function encodeConvertResolve(result) {
                port.log.debug && port.log.debug({message: result, $meta: {method: name, mtid: 'convert'}});
                packet[0] = result;
                return packet;
            });
    } else {
        return Promise.resolve(packet);
    }
};

const portDispatch = port => packet => {
    let $meta = (packet.length && packet[packet.length - 1]) || {};
    if ($meta && $meta.dispatch) {
        return Promise.resolve($meta.dispatch.apply(this, arguments));
    }
    if (!packet || !packet[0] || packet[0] === DISCARD) {
        return Promise.resolve(DISCARD);
    }
    let mtid = $meta.mtid;
    let opcode = $meta.opcode;
    let methodName = $meta.method;
    let startTime = hrtime();
    if ($meta.startTime && port.latency) {
        let diff = hrtime($meta.startTime);
        diff = diff[0] * 1000 + diff[1] / 1000000;
        port.latency(diff, 1);
    }

    let portDispatchResult = isError => result => {
        if (mtid === 'request' && $meta.mtid !== 'discard') {
            !$meta.mtid && ($meta.mtid = isError ? 'error' : 'response');
            !$meta.opcode && ($meta.opcode = opcode);
            if (!isError) {
                let diff = hrtime(startTime);
                diff = diff[0] * 1000 + diff[1] / 1000000;
                $meta.timeTaken = diff;
                if (methodName && port.timeTaken) {
                    port.timeTaken(methodName, {m: methodName}, diff, 1);
                }
            } else {
                port.error(result);
            }
            return [result, $meta];
        } else {
            return DISCARD;
        }
    };

    return Promise.resolve()
        .then(() => port.messageDispatch.apply(port, packet))
        .catch(portDispatchResult(true))
        .then(portDispatchResult(false));
};

Port.prototype.encodeStream = function encode(context, concurrency) {
    return this.createStream(
        packet => portSend(this, context)(packet)
            .then(portEncode(this, context))
            .catch(portErrorCallback(this, packet.length && packet[packet.length - 1])),
        concurrency,
        this.activeSendCount
    );
};

Port.prototype.disconnect = function(reason) {
    this.error(reason);
    throw errors.disconnect(reason);
};

const portQueueEventCreate = (port, context, name) => {
    context && port.log.info && port.log.info({$meta: {mtid: 'event', opcode: 'port.' + name}, connection: context});
    return [false, {mtid: 'event', opcode: name}];
};

const portQueueEvent = (port, context, encode, decode) => name => {
    return Promise.resolve(portQueueEventCreate(port, context, name))
        .then(portReceive(port, context))
        .then(packet => {
            if (packet[0]) {
                let $meta = (packet.length && packet[packet.length - 1]) || {};
                if ($meta.loopback) {
                    delete $meta.loopback;
                    encode.write(packet);
                } else {
                    decode.push(packet);
                }
            }
            return packet;
        });
};

Port.prototype.pipe = function pipe(stream, context) {
    return this.pull(stream, context);
};

Port.prototype._pipe = function pipe(stream, context) {
    let encode = this.encodeStream(context);
    let decode = this.decodeStream(context);
    let event = portQueueEvent(this, context, encode, decode);
    let queue = this.portQueues.create({
        context,
        config: this.config.queue,
        event,
        counter: this.counter,
        end: () => stream.end(),
        log: this.log
    });
    let streamSequence = [queue.stream(), encode, stream, decode];
    function unpipe() {
        return streamSequence.reduce(function unpipeStream(prev, next) {
            return next ? prev.unpipe(next) : prev;
        });
    }
    let conId = context && context.conId && context.conId.toString();
    if (conId) {
        if (this.socketTimeOut) {
            // TODO: This can be moved to ut-port-tcp as it is net.Socket specific functionality
            stream.setTimeout(this.socketTimeOut, handleStreamClose(this, stream, conId, unpipe));
        }
    }
    let receiveTimer;
    let restartReceiveTimer = () => {
        clearTimeout(receiveTimer);
        let receiveTimeout = this.config && this.config.receiveTimeout;
        if (receiveTimeout > 0) {
            receiveTimer = setTimeout(() => {
                this.log && this.log.error && this.log.error(errors.receiveTimeout());
                stream.end();
            }, receiveTimeout);
        }
    };
    restartReceiveTimer();
    stream
        .on('close', () => {
            event('disconnected');
        })
        .on('end', () => {
            clearTimeout(receiveTimer);
            handleStreamClose(this, undefined, conId, unpipe);
        })
        .on('error', error => {
            this.error(errors.stream({context, error}));
            handleStreamClose(this, stream, conId, unpipe);
        })
        .on('data', () => {
            restartReceiveTimer();
            queue.ping();
        });
    streamSequence
        .reduce(function pipeStream(prev, next) {
            return next ? prev.pipe(next) : prev;
        })
        .on('data', packet => {
            if (packet[0] instanceof errors.disconnect) {
                return stream.end();
            }
            return portDispatch(this)(packet).then(msg => {
                (msg !== DISCARD) && queue.push(msg);
            });
        });
    event('connected');
    return streamSequence.slice(1);
};

Port.prototype.pipeReverse = function pipeReverse(stream, context) {
    let self = this;
    let concurrency = this.config.concurrency || UNLIMITED;
    let callStream = this.createStream(function pipeReverseThrough(packet) {
        let $meta = (packet.length && packet[packet.length - 1]) || {};
        if ($meta.mtid === 'error' || $meta.mtid === 'response') {
            return packet;
        } else if ($meta.mtid === 'request') {
            // todo maybe this cb preserving logic is not needed
            let cb;
            if ($meta.callback) {
                cb = $meta.callback;
                delete $meta.callback;
            }
            let methodName = $meta.method;
            let startTime = hrtime();
            let push = function pipeReverseInStreamCb(result) {
                if (cb) {
                    $meta.callback = cb;
                }
                let diff = hrtime(startTime);
                diff = diff[0] * 1000 + diff[1] / 1000000;
                if ($meta) {
                    $meta.timeTaken = diff;
                }
                if (methodName && self.timeTaken) {
                    self.timeTaken(methodName, {m: methodName}, diff, 1);
                }
                return [result, $meta];
            };
            return Promise.resolve()
                .then(function pipeReverseInStream() {
                    return self.messageDispatch.apply(self, packet);
                })
                .then(push)
                .catch(push);
        } else {
            self.messageDispatch.apply(self, packet);
            return DISCARD;
        }
    }, concurrency, this.activeExecCount);

    [stream, this.decodeStream(context, concurrency), callStream, this.encodeStream(context, concurrency)]
        .reduce(function pipeReverseReduce(prev, next) {
            return next ? prev.pipe(next) : prev;
        })
        .on('data', function pipeReverseQueueData(packet) {
            self.messageDispatch.apply(self, packet);
        });
    this.streams.push(stream);
    return stream;
};

const portExec = (port, fn) => chunk => {
    let $meta = chunk.length > 1 && chunk[chunk.length - 1];
    let startTime = hrtime();
    let methodName = '';
    if ($meta && $meta.mtid === 'request') {
        $meta.mtid = 'response';
        methodName = $meta.method;
    }
    return Promise.resolve()
        .then(function pipeExecThrough() {
            return fn.apply(port, chunk);
        })
        .then(function pipeExecThroughResolved(result) {
            let diff = hrtime(startTime);
            diff = diff[0] * 1000 + diff[1] / 1000000;
            if ($meta) {
                $meta.timeTaken = diff;
            }
            if (methodName && port.timeTaken) {
                port.timeTaken(methodName, {m: methodName}, diff, 1);
            }
            return [result, $meta];
        })
        .catch(function pipeExecThroughRejected(error) {
            port.error(error);
            let diff = hrtime(startTime);
            diff = diff[0] * 1000 + diff[1] / 1000000;
            if ($meta) {
                $meta.timeTaken = diff;
                $meta.mtid = 'error';
            }
            if (methodName && port.timeTaken) {
                port.timeTaken(methodName, {m: methodName}, diff, 1);
            }
            return [error, $meta];
        });
};

Port.prototype.pipeExec = function pipeExec(fn) {
    return this.pull(fn);
};

Port.prototype._pipeExec = function pipeExec(fn) {
    let stream = this.createStream(portExec(this, fn), this.config.concurrency, this.activeExecCount);
    this.streams.push(stream);
    return this.pipe(stream);
};

Port.prototype.isDebug = function isDebug() {
    return this.config.debug || (this.config.debug == null && this.bus.config && this.bus.config.debug);
};

Port.prototype.includesConfig = function includesConfig(name, values, defaultValue) {
    let configValue = this.config[name];
    if (configValue == null) {
        return defaultValue;
    }
    if (!Array.isArray(values)) {
        values = [values];
    }
    return includes(configValue, values);
};

const portSink = queue => pull.drain(msg => {
    if (msg !== DISCARD) {
        queue.push(msg);
    }
}, end => {

});

const portDisconnect = () => pull.map(packet => {
    if (packet && packet[0] instanceof errors.disconnect) {
        throw packet[0];
    } else {
        return packet;
    }
});

Port.prototype.pull = function portPull(what, context) {
    let stream;
    if (typeof what === 'function') {
        stream = pullPromise.through(portExec(this, what));
    } else {
        stream = what;
        stream.on('close', () => {
            pull(
                pull.once(portQueueEventCreate(this, context, 'disconnected')),
                pullPromise.through(portReceive(this, context)),
                pullPromise.through(portDispatch(this)),
                pull.drain()
            );
        });
        stream.on('error', error => {
            this.error(errors.stream({context, error}));
        });
        this.socketTimeOut && stream.setTimeout(this.socketTimeOut, () => {
            stream.end();
        });
        stream = pullStream.duplex(stream);
    }
    let buffer = bufferCreate(0);
    let queue = this.portQueues.create({context});
    let send = pullPromise.through(portSend(this, context));
    let encode = pullPromise.through(portEncode(this, context));
    let decode = pullPromise.through(portDecode(this, context, buffer));
    let receive = pullPromise.through(portReceive(this, context));
    let disconnect = portDisconnect();
    let dispatch = pullPromise.through(portDispatch(this));
    let sink = portSink(queue);
    pull(
        pullCat([
            pull.once(portQueueEventCreate(this, context, 'connected')),
            pull(queue, send, encode, stream, decode)
        ]),
        receive,
        disconnect,
        dispatch,
        sink
    );
};

module.exports = Port;
