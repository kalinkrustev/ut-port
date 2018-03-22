'use strict';
const os = require('os');
const includes = require('./includes');
const utqueue = require('ut-queue');
const portStreams = require('./pull');
const timing = require('./timing');

const codecFlow = {
    encode: [['beforeEncode'], ['codec', 'encode'], ['afterEncode']],
    decode: [['beforeDecode'], ['codec', 'decode'], ['afterDecode']]
};

const getCodecFlow = function(port, type) {
    if (!codecFlow[type]) {
        throw port.errors.codecFlowNotFound({type});
    }
    var ef = codecFlow[type].reduce((accum, f) => { // search for methods in port, get the method and correct context, then exec the method within context
        let callCtx = port; // default method
        let callMethod = port[f[0]]; // default method ctx

        if (f.length > 1 && port[f[0]] && port[f[0]][f[1]]) { // if there is method called from codec like this port.codec.encode, ctx should be codec not port
            callCtx = port[f[0]];
            callMethod = port[f[0]][f[1]];
        }
        callMethod && accum.push((encodePacket, $meta, context) => {
            return Promise.resolve(callMethod.call(callCtx, encodePacket, $meta, context)); // call the method within ctx
        });
        return accum;
    }, []);
    var efLen = ef.length;
    return (encodePacket, $meta, context) => {
        if (!efLen) {
            return encodePacket;
        }
        return ef.reduce((p, f) => {
            return p.then(p => f(p, $meta, context));
        }, Promise.resolve(encodePacket[0]));
    };
};

function Port(params) {
    this.log = {};
    this.logFactory = (params && params.logFactory) || null;
    this.bus = (params && params.bus) || null;
    let defineError = this.defineError = (this.bus && this.bus.errors.defineError) || params.defineError;
    this.getError = (this.bus && this.bus.errors.getError) || params.getError;
    let PortError = defineError('port');
    this.errors = {
        missingParams: defineError('missingParameters', PortError, 'Missing parameters'),
        missingMeta: defineError('missingMeta', PortError, 'Missing metadata'),
        notConnected: defineError('notConnected', PortError, 'No connection'),
        disconnect: defineError('disconnect', PortError, 'Port disconnected'),
        disconnectBeforeResponse: defineError('disconnectBeforeResponse', PortError, 'Disconnect before response received'),
        stream: defineError('stream', PortError, 'Port stream error'),
        timeout: defineError('timeout', PortError, 'Timeot'),
        echoTimeout: defineError('echoTimeout', PortError, 'Echo retries limit exceeded'),
        unhandled: defineError('unhandled', PortError, 'Unhandled port error'),
        bufferOverflow: defineError('bufferOverflow', PortError, 'Message size of {size} exceeds the maximum of {max}'),
        socketTimeout: defineError('socketTimeout', PortError, 'Socket timeout'),
        receiveTimeout: defineError('receiveTimeout', PortError, 'Receive timeout'),
        dispatchFailure: defineError('dispatchFailure', PortError, 'Cannot dispatch message to bus'),
        queueNotFound: defineError('queueNotFound', PortError, 'Queue not found'),
        codecFlowNotFound: defineError('codecFlowNotFound', PortError, 'incorrect codec flow type')
    };

    this.sendQueues = utqueue.queues();
    this.receiveQueues = utqueue.queues();
    this.counter = null;
    this.streams = [];
    // performance metrics
    this.methodLatency = null;
    this.portLatency = null;
    this.activeExecCount = null;
    this.activeDispatchCount = null;
    this.activeSendCount = null;
    this.activeReceiveCount = null;
    this.msgSent = null;
    this.msgReceived = null;
    // codec related metrics
    this.activeEncodeCount = null;
    this.activeDecodeCount = null;
    this.bytesSent = null;
    this.bytesReceived = null;
    // state properties
    this.isReady = false;
    this.isConnected = new Promise(resolve => {
        this.resolveConnected = resolve;
    });
    this.state = 'stopped';
}

Port.prototype.timing = timing;

Port.prototype.init = function init() {
    this.logFactory && (this.log = this.logFactory.createLog(this.config.logLevel, {name: this.config.id, context: this.config.type + ' port'}, this.config.log));

    if (this.config.metrics !== false && this.bus && this.bus.config.implementation && this.bus.performance) {
        let measurementName = this.config.metrics || this.config.id;
        let tags = {
            host: os.hostname(),
            impl: this.bus.performance.config.id || this.bus.config.implementation,
            pid: process.pid
        };
        this.counter = function initCounters(fieldType, fieldCode, fieldName, interval) {
            return this.bus.performance.register(measurementName, fieldType, fieldCode, fieldName, 'standard', tags, interval);
        }.bind(this);
        this.methodLatency = this.bus.performance.register(
            measurementName + '_T',
            'average',
            ['q', 'r', 'e', 'x', 'd', 's', 'w'],
            'Method exec time', 'tagged',
            tags
        );
        this.portLatency = this.counter('average', 'lt', 'Port latency', 300);
        this.activeExecCount = this.counter('gauge', 'ae', 'Active exec count', 300);
        this.activeDispatchCount = this.counter('gauge', 'ad', 'Active dispatch count', 300);
        this.activeSendCount = this.counter('gauge', 'as', 'Active send count');
        this.activeReceiveCount = this.counter('gauge', 'ar', 'Active receive count');
        this.msgSent = this.counter('counter', 'ms', 'Messages sent', 300);
        this.msgReceived = this.counter('counter', 'mr', 'Messages received', 300);
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
    let args = Array.prototype.slice.call(arguments);
    let result = this.bus && this.bus.dispatch.apply(this.bus, args);
    if (!result) {
        this.log.error && this.log.error(this.errors.dispatchFailure({args}));
    }
    return result;
};

Port.prototype.start = function start() {
    this.state = 'starting';
    this.encodeFlow = getCodecFlow(this, 'encode');
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

Port.prototype.request = function request(...args) {
    return portStreams.portPush(this, true, args);
};

Port.prototype.publish = function publish(...args) {
    return portStreams.portPush(this, false, args);
};

Port.prototype.error = function portError(error) {
    this.log.error && this.log.error(error);
};

Port.prototype.fatal = function portError(error) {
    this.log.fatal && this.log.fatal(error);
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

Port.prototype.disconnect = function(reason) {
    this.error(reason);
    throw this.errors.disconnect(reason);
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

Port.prototype.pull = function pull(what, context) {
    let result = portStreams.portPull(this, what, context);
    this.resolveConnected(true);
    return result;
};

Port.prototype.setTimer = function setTimer($meta) {
    $meta.timer = portStreams.packetTimer($meta.method, '*', this.config.id, $meta.timeout);
};

module.exports = Port;
