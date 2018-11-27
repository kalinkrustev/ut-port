const merge = require('./merge');
const utPort = require('./port');

module.exports = ({bus, logFactory, assert}) => {
    let servicePorts = new Map();
    let index = 0;

    let params = config => ({
        utLog: logFactory,
        utBus: bus,
        utPort,
        parent: utPort,
        utError: bus.errors,
        utMethod: (...params) => bus.importMethod(...params),
        config
    });

    let createOne = (portConfig, envConfig) => {
        let Constructor;
        if (portConfig instanceof Function) {
            let id = portConfig.name;
            if (id) {
                let cfg = envConfig[id];
                portConfig = cfg !== false && cfg !== 'false' && portConfig(merge(cfg, envConfig.port));
                if (portConfig && portConfig.id && portConfig.id !== id) {
                    throw new Error(`Port id ${portConfig.id} does not match function name ${id}`);
                }
                if (portConfig && !portConfig.id) {
                    portConfig.id = id;
                }
            } else {
                portConfig = portConfig();
            }
        };
        if (!portConfig) {
            return false;
        } else if (!portConfig.id) {
            throw new Error('Missing port id property');
        } else if (envConfig[portConfig.id] === false || envConfig[portConfig.id] === 'false') { // port is disabled
            return false;
        }
        merge(portConfig, envConfig.port, envConfig[portConfig.id]);
        if (!(portConfig.createPort instanceof Function)) {
            if (portConfig.type) {
                throw new Error('Use createPort:require(\'ut-port-' + portConfig.type + '\') instead of type:\'' + portConfig.type + '\'');
            } else {
                throw new Error('Missing createPort property');
            }
        }
        Constructor = portConfig.createPort(params(portConfig));
        portConfig.order = portConfig.order || index;
        index++;
        let port = new Constructor(params(portConfig));
        return Promise.resolve(port.init()).then(function() {
            servicePorts.set(portConfig.id, port);
            return port;
        });
    };

    let createMany = (ports, envConfig) => ports.reduce(function(all, port) {
        port = port && createOne(port, envConfig);
        port && all.push(port);
        return all;
    }, []);

    let createAny = (items, envConfig) => items.map(async({create, moduleName}) => {
        let moduleConfig = moduleName ? envConfig[moduleName] : envConfig;
        let config = create.name ? (moduleConfig || {})[create.name] : moduleConfig;
        let Result;
        if (config !== false && config !== 'false') {
            index++;
            Result = create(params(config));
            if (Result instanceof Function) { // item returned a constructor
                if (!Result.name) throw new Error('Missing constructor name for port');
                config = (moduleConfig || {})[Result.name] || {};
                config.order = config.order || index;
                config.id = moduleName ? moduleName + '.' + Result.name : Result.name;
                Result = new Result(params(config));
                servicePorts.set(config.id, Result);
            } else if (Result instanceof Object && create.name) {
                bus.registerLocal(Result, moduleName ? moduleName + '.' + create.name : create.name);
            }
            await (Result && Result.init instanceof Function) && Result.init();
        }
        return Result;
    });

    let create = (ports, any, envConfig) => Promise.all([].concat(
        createAny(any, envConfig),
        Array.isArray(ports) ? createMany(ports, envConfig, assert) : createOne(ports, envConfig, assert)
    ).filter(item => item));

    let fetch = ports => Array.from(servicePorts.values()).sort((a, b) => a.config.order > b.config.order ? 1 : -1);

    let startOne = async({port}) => {
        port = servicePorts.get(port);
        await port && port.start();
        await port && port.ready();
        return port;
    };

    let startMany = ports => {
        var portsStarted = [];
        return fetch(ports).reduce(function(prev, port) {
            portsStarted.push(port); // collect ports that are started
            return prev
                .then(() => port.start())
                .then(result => {
                    assert && assert.ok(true, 'started port ' + port.config.id);
                    return result;
                });
        }, Promise.resolve())
            .then(function() {
                return portsStarted
                    .reduce(function(promise, port) {
                        if (typeof port.ready === 'function') {
                            promise = promise.then(() => port.ready());
                        }
                        return promise;
                    }, Promise.resolve())
                    .then(() => portsStarted);
            })
            .catch(function(err) {
                return portsStarted.reverse().reduce(function(prev, context, idx) {
                    return prev
                        .then(() => context.stop())
                        .catch(() => true); // continue on error
                }, Promise.resolve())
                    .then(() => Promise.reject(err)); // reject with the original error
            });
    };

    let start = params =>
        Array.isArray(params || []) ? startMany(params) : startOne(params);

    var port = {
        get: ({port}) => servicePorts.get(port),
        fetch,
        create,
        start,
        stop: async({port}) => {
            port = servicePorts.get(port);
            await port && port.stop();
            return port;
        },
        move: ({port, x, y}) => {
            port = servicePorts.get(port);
            if (port) {
                port.config.x = x;
                port.config.y = y;
            }
            return port;
        }
    };

    bus.registerLocal({port}, 'ut');

    return port;
};
