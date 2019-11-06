const utPort = require('./port');

module.exports = ({bus, logFactory, assert}) => {
    let servicePorts = new Map();
    let serviceModules = new Map();
    let index = 0;
    let modules = {};

    let params = (config, base) => ({
        utLog: logFactory,
        utBus: bus,
        utPort: base,
        utError: bus.errors,
        registerErrors: bus.registerErrors,
        utMethod: (...params) => bus.importMethod(...params),
        utNotify: (...params) => bus.notification(...params),
        config
    });

    let createItem = async({create, moduleName, pkg}, envConfig, base) => {
        let moduleConfig = moduleName ? envConfig[moduleName] : envConfig;
        modules[moduleName || '.'] = modules[moduleName || '.'] || [];
        let config = create.name ? (moduleConfig || {})[create.name] : moduleConfig;
        let Result;
        if (config !== false && config !== 'false') {
            index++;
            Result = await create(params(config, base));
            if (Result instanceof Function) { // item returned a constructor
                if (!Result.name) throw new Error(`Module "${moduleName}${create.name ? '/' + create.name : ''}" returned anonymous constructor:\n${Result}`);
                config = (moduleConfig || {})[Result.name];
                if (config === false || config === 'false') {
                    return;
                } else {
                    config = config || {};
                    if (typeof config !== 'object') config = {};
                    config.order = config.order || index;
                    config.id = (moduleName ? moduleName + '.' + Result.name : Result.name);
                    config.pkg = pkg;
                    Result = new Result(params(config, base));
                    servicePorts.set(config.id, Result);
                }
            } else if (Result instanceof Object) {
                if (!create.name) throw new Error(`Module "${moduleName}" returned plain object from anonymous function:\n${create}`);
                let id = moduleName ? moduleName + '.' + create.name : create.name;
                bus.registerLocal(Result, id, pkg);
                Result = {
                    destroy() {
                        serviceModules.delete(id);
                        bus.unregisterLocal(id);
                    },
                    start() {
                    },
                    config: {
                        id,
                        type: 'module',
                        order: index,
                        pkg
                    },
                    init: Result.init
                };
                serviceModules.set(id, Result);
            } else if (Result) {
                throw new Error(`Module "${moduleName}" returned unexpected value:\n${Result}`);
            }
            await (Result && Result.init instanceof Function && Result.init());
            Result && modules[moduleName || '.'].push(Result);
        }
        return Result;
    };

    let create = async(items, envConfig) => {
        let result = [];
        let base = utPort(envConfig.utPort);
        for (const item of items) result.push(await createItem(item, envConfig, base));
        return result.filter(item => item);
    };

    let fetch = filter =>
        Array.from(servicePorts.values())
            .concat(Array.from(serviceModules.values()))
            .sort((a, b) => a.config.order > b.config.order ? 1 : -1);

    let startOne = async({port}) => {
        port = servicePorts.get(port);
        await (port && port.start());
        await (port && port.ready());
        return port;
    };

    let startMany = async ports => {
        let portsStarted = [];
        try {
            for (let port of ports) {
                portsStarted.push(port); // collect ports that are started
                port = await port.start();
                assert && assert.ok(true, 'started port ' + port.config.id);
            }
            for (let port of portsStarted) {
                await (port.ready instanceof Function && port.ready());
            }
        } catch (error) {
            for (let port of portsStarted) {
                try {
                    await (port.stop instanceof Function && port.stop());
                } catch (ignore) { /* just continue calling stop */ };
            }
            throw error;
        }
        return portsStarted;
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
            await (port && port.stop());
            return port;
        },
        destroy: async moduleName => {
            let started = modules[moduleName || '.'];
            if (started) {
                for (let item of started) {
                    await item.destroy();
                }
            }
            delete modules[moduleName || '.'];
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
