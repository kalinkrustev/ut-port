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
const addFormats = require('ajv-formats');

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
        }, config, merge({}, this.defaults?.mergeOptions, config.mergeOptions));
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
                test: {
                    readOnly: true,
                    type: 'boolean',
                    default: false
                },
                watch: {
                    readOnly: true,
                    oneOf: [{type: 'string'}, {type: 'object'}, {type: 'boolean'}, {type: 'null'}],
                    default: false
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
                },
                noRecursion: {
                    readOnly: true,
                    enum: [false, 'trace', 'debug', 'info', 'warn', 'error', true],
                    default: false
                },
                mergeOptions: {
                    type: 'object',
                    properties: {
                        mergeStrategies: {
                            type: 'object'
                        }
                    }
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
                impl: this.bus.performance.config.impl || this.bus.config.implementation || this.bus.performance.config.id
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
        const id = this.config.id.replace(/\./g, '-');
        return this.bus && Promise.all([
            this.bus.register({[`${id}.start`]: this.start, [`${id}.stop`]: this.stop}, 'ports', this.config.id, this.config.pkg),
            this.bus.subscribe({[`${id}.drain`]: this.drain.bind(this)}, 'ports', this.config.id, this.config.pkg),
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

    forNamespaces(reducer, initial) {
        const id = this.config.id.replace(/\./g, '-');
        return [].concat(this.config.namespace || this.config.imports || id).reduce(reducer.bind(this), initial);
    }

    async start() {
        const ajv = new Ajv({allErrors: true, verbose: true});
        addFormats(ajv);
        const validate = ajv.compile(this.configSchema);
        const valid = validate(this.config);
        if (!valid) {
            throw this.errors['port.configValidation']({
                errors: validate.errors,
                params: {
                    message: [].concat(validate.errors).map(error => this.config.id + error.instancePath + ' ' + error.message).join('\r\n')
                }
            });
        }
        this.state = 'starting';

        const {req, pub} = this.forNamespaces(function startPortNamespaces(prev, next) {
            if (typeof next === 'string') {
                prev.req[`${next}.request`] = this.request.bind(this);
                prev.pub[`${next}.publish`] = this.publish.bind(this);
            }
            return prev;
        }, {req: {}, pub: {}});
        await (this.bus && Promise.all([
            this.bus.register(req, 'ports', this.config.id, this.config.pkg),
            this.bus.subscribe(pub, 'ports', this.config.id, this.config.pkg)
        ]));

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
                }
                break;
        }
        await (this.bus && typeof this.bus.portEvent instanceof Function && this.bus.portEvent(event, this));
        return result;
    }

    async stop() {
        this.state = 'stopping';

        const {req, pub} = this.forNamespaces(function stopPortNamespaces(prev, next) {
            prev.req.push(`${next}.request`);
            prev.pub.push(`${next}.publish`);
            return prev;
        }, {req: [], pub: []});
        this.bus.unregister(req, 'ports', this.config.id);
        this.bus.unsubscribe(pub, 'ports', this.config.id);

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
        const id = this.config.id.replace(/\./g, '-');
        this.bus.unregister([`${id}.start`, `${id}.stop`], 'ports', this.config.id);
        this.bus.unsubscribe([`${id}.drain`], 'ports', this.config.id);
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
        if (schema.validate) {
            if (this.config.test && schema.artifact) schema = schema.artifact(`${method}.${type}`);
            const abortEarly = !this.isDebug();
            return $meta => value => {
                const {error, value: result, warning, artifacts} = schema.validate(value, {
                    abortEarly
                });
                if (error) {
                    throw this.errors[`port.${type}Validation`]({
                        cause: error,
                        params: {
                            method,
                            type,
                            fields: error.details?.map(item => item.path?.join('.')).filter(Boolean).join(', ')
                        }
                    });
                }

                if (this.config.test && artifacts) $meta.validation = Object.fromEntries(artifacts);
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
            return validation && validation[type] && validation[type]($meta);
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

    importNamespaces() {
        if (this.methods.importedMap) {
            const importedNamespaces = Array.from(this.methods.importedMap.values()).reduce((prev, {
                namespace
            }) => namespace ? prev.concat(namespace) : prev, []);
            if (importedNamespaces.length) {
                this.config.namespace = Array.from(new Set([].concat(this.config.namespace, importedNamespaces).filter(Boolean)));
            }
        }
    }

    get timing() { return timing; }
    get merge() { return merge; }
};
