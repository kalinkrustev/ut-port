'use strict';
const os = require('os');
const includes = require('./includes');
const utQueue = require('ut-queue');
const portStreams = require('./pull');
const timing = require('./timing');
const merge = require('ut-function.merge');
const createErrors = require('./errors');
const EventEmitter = require('events');
const Ajv = require('ajv');
module.exports = (defaults) => class Port extends EventEmitter {
    constructor({ utLog, utBus, utError, config } = {}) {
        super();
        this.log = {};
        this.utLog = utLog;
        this.bus = utBus;
        this.errors = createErrors(utError);
        this.config = this.traverse(obj => {
            if (Object.prototype.hasOwnProperty.call(obj, 'defaults')) {
                const result = obj.defaults;
                return result instanceof Function ? result.apply(this) : result;
            }
        }, config);
        this.configSchema = this.traverse(obj => {
            if (Object.prototype.hasOwnProperty.call(obj, 'schema')) {
                const result = obj.schema;
                return result instanceof Function ? result.apply(this) : result;
            }
        }, {});

        this.configUiSchema = this.traverse(obj => {
            if (Object.prototype.hasOwnProperty.call(obj, 'uiSchema')) {
                const result = obj.uiSchema;
                return result instanceof Function ? result.apply(this) : result;
            }
        }, {});

        this.methods = {};
        this.methodValidations = {};
        this.validationsCache = {};
        this.sendQueues = utQueue.queues();
        this.receiveQueues = utQueue.queues();
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

    get schema() {
        return {
            type: 'object',
            properties: {
                logLevel: {
                    readOnly: true,
                    type: 'string',
                    enum: ['off', 'fatal', 'error', 'warn', 'info', 'debug', 'trace']
                },
                disconnectOnError: {
                    readOnly: true,
                    type: 'boolean',
                    default: true
                },
                id: {
                    readOnly: true,
                    type: 'string'
                },
                type: {
                    readOnly: true,
                    type: 'string'
                },
                namespace: {
                    readOnly: true,
                    oneOf: [
                        {
                            type: 'string'
                        },
                        {
                            type: 'array',
                            items: {
                                type: 'string'
                            }
                        }
                    ]
                },
                imports: {
                    readOnly: true,
                    type: 'array',
                    items: {
                        oneOf: [{
                            type: 'string'
                        }, {
                            type: 'object' // can be regex
                        }]
                    }
                },
                metrics: {
                    readOnly: true,
                    oneOf: [
                        {
                            enum: [false]
                        },
                        {
                            type: 'string'
                        }
                    ]
                }
            }
        };
    }

    traverse(prop, initial, mergeOptions) {
        const config = [initial];
        for (let current = Object.getPrototypeOf(this); current; current = Object.getPrototypeOf(current)) {
            const value = prop(current);
            if (value) config.push(value);
        }
        return merge(config.reverse(), mergeOptions);
    }

    defaults() {
        return {
            ...{
                logLevel: 'info',
                disconnectOnError: true
            },
            ...defaults
        };
    }

    init() {
        this.methods = this.traverse(obj => {
            if (Object.prototype.hasOwnProperty.call(obj, 'handlers')) {
                const result = obj.handlers;
                return result instanceof Function ? result.apply(this) : result;
            }
        }, {});
        this.methodValidations = this.traverse(obj => {
            if (Object.prototype.hasOwnProperty.call(obj, 'validations')) {
                const result = obj.validations;
                return result instanceof Function ? result.apply(this) : result;
            }
        }, {});
        this.utLog && (this.log = this.utLog.createLog(this.config.logLevel, { name: this.config.id, context: this.config.type + ' port' }, this.config.log));
        if (this.config.metrics !== false && this.bus && this.bus.config.implementation && this.bus.performance) {
            let measurementName = this.config.metrics || this.config.id;
            let taggedMeasurementName = measurementName + '_T';
            const tags = {
                hostname: os.hostname(),
                env: this.bus.config.params && this.bus.config.params.env,
                location: this.bus.config.location,
                context: this.config.type + ' port',
                impl: this.bus.performance.config.impl || this.bus.performance.config.id || this.bus.config.implementation
            };
            if (this.bus.performance.config.prometheus) {
                tags.name = measurementName;
                measurementName = 'count';
                taggedMeasurementName = 'time';
            }
            if (this.bus.config.service) {
                tags.service = this.bus.config.service;
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
            this.activeExecCount = this.counter('gauge', 'ax', 'Active exec count', 300);
            this.activeDispatchCount = this.counter('gauge', 'aw', 'Active dispatch count', 300);
            this.activeSendCount = this.counter('gauge', 'as', 'Active send count');
            this.activeReceiveCount = this.counter('gauge', 'ar', 'Active receive count');
            this.msgSent = this.counter('counter', 'ms', 'Messages sent', 300);
            this.msgReceived = this.counter('counter', 'mr', 'Messages received', 300);
        }
        const methods = { req: {}, pub: {} };
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
            this.bus.register(methods.req, 'ports', this.config.id, this.config.pkg),
            this.bus.subscribe(methods.pub, 'ports', this.config.id, this.config.pkg),
            this.bus && typeof this.bus.portEvent instanceof Function && this.bus.portEvent('init', this)
        ]);
    }

    messageDispatch() {
        const args = Array.prototype.slice.call(arguments);
        const result = this.bus && this.bus.dispatch.apply(this.bus, args);
        if (!result) {
            this.log.error && this.log.error(this.errors['port.dispatchFailure']({ args }));
        }
        return result;
    }

    start() {
        const ajv = new Ajv({allErrors: true, verbose: true});
        const validate = ajv.compile(this.configSchema);
        const valid = validate(this.config);
        if (!valid) throw this.errors['port.configValidation']({errors: validate.errors});
        this.state = 'starting';
        return this.fireEvent('start', { config: this.config });
    }

    async ready() {
        const result = await this.fireEvent('ready');
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
        const eventHandlers = this.methods[event] ? [this.methods[event]] : [];
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
                for (const eventHandler of eventHandlers) {
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
        const methods = [].concat(this.config.namespace || this.config.imports || this.config.id).reduce(function destroyReduceMethods(prev, next) {
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
        if (!this.methods.imported && this.methods.importedMap) throw new Error('Incorrect ut-bus version, please use 7.11.3 or newer');
        const result = this.methods.imported && this.methods.imported[methodName];
        return result || this.methods[methodName];
    }

    validator(schema, method, type) {
        if (!schema) return;
        if (schema.isJoi) {
            const abortEarly = !this.isDebug();
            return value => {
                const {error, value: result, warning} = schema.validate(value, {
                    abortEarly
                });
                if (error) {
                    throw this.errors[`port.${type}Validation`]({
                        cause: error,
                        params: {
                            method,
                            type
                        }
                    });
                }
                warning && this.log.warn && this.log.warn({
                    warning,
                    $meta: {
                        mtid: 'validation',
                        method
                    }
                });
                return result;
            };
        }
    }

    findValidation($meta) {
        const method = $meta && $meta.method && ($meta.method);
        const type = $meta && {
            request: 'params',
            notification: 'params',
            response: 'result'
        }[$meta.mtid];
        if (method && type) {
            let validation = this.validationsCache[method];
            if (!validation) {
                validation = method && ((this.methodValidations.imported && this.methodValidations.imported[method]) || this.methodValidations[method]);
                if (typeof validation === 'function') validation = validation();
                validation = this.validationsCache[method] = validation && {
                    params: this.validator(validation.params, method, 'params'),
                    result: this.validator(validation.result, method, 'result')
                };
            }
            return validation && validation[type];
        }
    }

    getConversion($meta, type) {
        let fn;
        let name;
        if ($meta) {
            if ($meta.method) {
                const path = this.bus.getPath($meta.method);
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
            if (!fn) {
                name = [$meta.mtid, type].join('.');
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
        const configValue = this.config[name];
        if (configValue == null) {
            return defaultValue;
        }
        if (!Array.isArray(values)) {
            values = [values];
        }
        return includes(configValue, values);
    }

    pull(what, context) {
        const result = portStreams.portPull(this, what, context);
        this.resolveConnected(true);
        return result;
    }

    setTimer($meta) {
        $meta.timer = portStreams.packetTimer(this.bus.getPath($meta.method), '*', this.config.id, $meta.timeout);
    }

    get timing() { return timing; }
    get merge() { return merge; }
};
