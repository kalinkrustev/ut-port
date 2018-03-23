const mergeWith = require('lodash.mergewith');
module.exports = function merge(...args) {
    return mergeWith(...args, (targetVal, sourceVal) => {
        if (Array.isArray(targetVal) && sourceVal) {
            if (Array.isArray(sourceVal)) {
                return sourceVal;
            }
            if (sourceVal instanceof Set) {
                return targetVal
                    .concat(Array.from(sourceVal))
                    .filter((value, index, arr) => {
                        return value && arr.indexOf(value) === index;
                    });
            }
        }
    });
};
