const uuid = require('uuid').v4;
module.exports = {
    utMeta: ($meta = {}) => ({
        ...$meta,
        forward: {
            ...$meta.forward,
            'x-b3-traceid': uuid().replace(/-/g, ''),
            'x-ut-stack': undefined
        }
    })
};
