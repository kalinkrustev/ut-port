'use strict';
const os = require('os');
const includes = require('./includes');
const utqueue = require('ut-queue');
const portStreams = require('./pull');
const timing = require('./timing');
const merge = require('ut-function.merge');
const createErrors = require('./errors');
const EventEmitter = require('events');

module.exports = (defaults) => class Port extends EventEmitter {
    constructor({ utLog, utBus, utError, config } = {}) {
        super();
        this.log = {};
        this.utLog = utLog;
        this.bus = utBus;
        this.errors = createErrors(utError);
        this.config = this.traverse(obj => {
            if (obj.hasOwnProperty('defaults')) {
                let result = obj.defaults;
                return result instanceof Function ? result.apply(this) : result;
            }
        }, config);
        this.methods = {};
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
    traverse(prop, initial) {
        let config = [initial];
        for (let current = Object.getPrototypeOf(this); current; current = Object.getPrototypeOf(current)) {
            let value = prop(current);
            if (value) config.push(value);
        }
        return merge(config.reverse());
    }
    defaults() {
        return {...{
            logLevel: 'info',
            disconnectOnError: true
        },
        ...defaults};
    }
    init() {
        this.methods = this.traverse(obj => {
            if (obj.hasOwnProperty('handlers')) {
                let result = obj.handlers;
                return result instanceof Function ? result.apply(this) : result;
            }
        }, {});
        this.utLog && (this.log = this.utLog.createLog(this.config.logLevel, { name: this.config.id, context: this.config.type + ' port' }, this.config.log));
        if (this.config.metrics !== false && this.bus && this.bus.config.implementation && this.bus.performance) {
            let measurementName = this.config.metrics || this.config.id;
            let taggedMeasurementName = measurementName + '_T';
            let tags = {
                host: os.hostname(),
                impl: this.bus.performance.config.impl || this.bus.performance.config.id || this.bus.config.implementation
            };
            if (this.bus.performance.config.prometheus) {
                tags.port = measurementName;
                measurementName = 'count';
                taggedMeasurementName = 'time';
            }
            if (this.bus.config.service) {
                tags.svc = this.bus.config.service;
            } else {
                tags.pid = process.pid;
            }
            this.counter = function initCounters(fieldType, fieldCode, fieldName, interval) {
                return this.bus.performance.register(measurementName, fieldType, fieldCode, this.config.id + ': ' + fieldName, 'standard', tags, interval);
            }.bind(this);
            this.methodLatency = this.bus.performance.register(
                taggedMeasurementName,
                'average',
                ['q', 'r', 'e', 'x', 'd', 's', 'w'],
                this.config.id + ': Method exec',
                'tagged',
                tags,
                undefined,
                [
                    {help: 'queue time', code: 'q'},
                    {help: 'receive time', code: 'r'},
                    {help: 'encode time', code: 'e'},
                    {help: 'execute time', code: 'x'},
                    {help: 'decode time', code: 'd'},
                    {help: 'send time', code: 's'},
                    {help: 'dispatch time', code: 'w'}
                ]
            );
            this.portLatency = this.counter('average', 'lt', 'Port latency', 300);
            this.activeExecCount = this.counter('gauge', 'ae', 'Active exec count', 300);
            this.activeDispatchCount = this.counter('gauge', 'ad', 'Active dispatch count', 300);
            this.activeSendCount = this.counter('gauge', 'as', 'Active send count');
            this.activeReceiveCount = this.counter('gauge', 'ar', 'Active receive count');
            this.msgSent = this.counter('counter', 'ms', 'Messages sent', 300);
            this.msgReceived = this.counter('counter', 'mr', 'Messages received', 300);
        }
        let methods = { req: {}, pub: {} };
        const id = this.config.id.replace(/\./g, '-');
        methods.req[id + '.start'] = this.start;
        methods.req[id + '.stop'] = this.stop;
        methods.pub[id + '.drain'] = this.drain.bind(this);
        [].concat(this.config.namespace || this.config.imports || id).reduce(function initReduceMethods(prev, next) {
            if (typeof next === 'string') {
                prev.req[next + '.request'] = this.request.bind(this);
                prev.pub[next + '.publish'] = this.publish.bind(this);
            }
            return prev;
        }.bind(this), methods);
        return this.bus && Promise.all([
            this.bus.register(methods.req, 'ports', this.config.id),
            this.bus.subscribe(methods.pub, 'ports', this.config.id),
            this.bus && typeof this.bus.portEvent instanceof Function && this.bus.portEvent('init', this)
        ]);
    }
    messageDispatch() {
        let args = Array.prototype.slice.call(arguments);
        let result = this.bus && this.bus.dispatch.apply(this.bus, args);
        if (!result) {
            this.log.error && this.log.error(this.errors['port.dispatchFailure']({ args }));
        }
        return result;
    }
    start() {
        this.state = 'starting';
        return this.fireEvent('start', { config: this.config });
    }
    async ready() {
        let result = await this.fireEvent('ready');
        this.isReady = true;
        this.state = 'started';
        return result;
    }
    async fireEvent(event, data, mapper) {
        this.log.info && this.log.info(Object.assign({
            $meta: {
                mtid: 'event',
                method: `port.${event}`
            }
        }, data));
        let eventHandlers = this.methods[event] ? [this.methods[event]] : [];
        if (this.methods.importedMap) {
            Array.from(this.methods.importedMap.values()).forEach((imp) => {
                imp[event] && eventHandlers.push(imp[event]);
            });
        }
        let result = data;
        switch (mapper) {
            case 'asyncMap':
                result = await Promise.all(eventHandlers.map(handler => handler.call(this, data)));
                break;
            case 'reduce':
            default:
                for (let eventHandler of eventHandlers) {
                    result = await eventHandler.call(this, result);
                };
                break;
        }
        await (this.bus && typeof this.bus.portEvent instanceof Function && this.bus.portEvent(event, this));
        return result;
    }
    async stop() {
        this.state = 'stopping';
        await this.fireEvent('stop');
        this.removeAllListeners();
        this.streams.forEach(function streamEnd(stream) {
            stream.end();
        });
        this.sendQueues.end();
        this.state = 'stopped';
        return true;
    }

    async destroy() {
        await this.stop();
        let methods = [].concat(this.config.namespace || this.config.imports || this.config.id).reduce(function destroyReduceMethods(prev, next) {
            prev.req.push(next + '.request');
            prev.pub.push(next + '.publish');
            return prev;
        }, {req: [this.config.id + '.start', this.config.id + '.stop'], pub: [this.config.id + '.drain']});

        this.bus.unregister(methods.req, 'ports', this.config.id);
        this.bus.unsubscribe(methods.pub, 'ports', this.config.id);
    }
    drain(...args) {
        return portStreams.portDrain(this, args);
    }
    request(...args) {
        return portStreams.portPush(this, true, args);
    }
    publish(...args) {
        return portStreams.portPush(this, false, args);
    }
    error(error, $meta) {
        if (this.log.error) {
            if ($meta) error.method = $meta.method;
            this.log.error(error);
        }
    }
    fatal(error) {
        this.log.fatal && this.log.fatal(error);
    }
    methodPath(methodName) {
        return methodName.split('/', 2)[1];
    }
    findHandler(methodName) {
        let result;
        if (this.methods.importedMap) {
            for (let imported of this.methods.importedMap.values()) {
                result = imported[methodName];
                if (result) break;
            }
        }
        return result || this.methods[methodName];
    }
    getConversion($meta, type) {
        let fn;
        let name;
        if ($meta) {
            if ($meta.method) {
                let path = this.bus.getPath($meta.method);
                name = [path, $meta.mtid, type].join('.');
                fn = this.findHandler(name);
                if (!fn) {
                    name = [this.methodPath(path), $meta.mtid, type].join('.');
                    fn = this.findHandler(name);
                }
            }
            if (!fn) {
                name = [$meta.opcode, $meta.mtid, type].join('.');
                fn = this.findHandler(name);
            }
        }
        if (!fn && (!$meta || $meta.mtid !== 'event')) {
            name = type;
            fn = this.findHandler(name);
        }
        return { fn, name };
    }
    disconnect(reason) {
        this.error(reason);
        throw this.errors['port.disconnect'](reason);
    }
    isDebug() {
        return this.config.debug || (this.config.debug == null && this.bus.config && this.bus.config.debug);
    }
    includesConfig(name, values, defaultValue) {
        let configValue = this.config[name];
        if (configValue == null) {
            return defaultValue;
        }
        if (!Array.isArray(values)) {
            values = [values];
        }
        return includes(configValue, values);
    }
    pull(what, context) {
        let result = portStreams.portPull(this, what, context);
        this.resolveConnected(true);
        return result;
    }
    setTimer($meta) {
        $meta.timer = portStreams.packetTimer(this.bus.getPath($meta.method), '*', this.config.id, $meta.timeout);
    }
    get timing() { return timing; }
    get merge() { return merge; }
};
