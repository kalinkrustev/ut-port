const uuid = require('uuid').v4;
module.exports = {
    utMeta: ($meta = {}) => ({
        forward: {
            'x-b3-traceid': uuid().replace(/-/g, '')
        },
        ...$meta
    })
};
