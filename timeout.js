const timing = require('./timing');

function Timeout() {
    this.calls = new Set();
    this.interval = false;
}

Timeout.prototype.clean = function timeoutClean() {
    const now = timing.now();
    Array.from(this.calls).forEach(end => end.checkTimeout(now));
};

Timeout.prototype.startWait = function timeoutStartWait(onTimeout, timeout, createTimeoutError, set) {
    this.interval = this.interval || setInterval(this.clean.bind(this), 500);
    const end = error => {
        this.endWait(end, set);
        error && onTimeout(error);
    };
    end.checkTimeout = time => timing.isAfter(time, timeout) && end(createTimeoutError());
    this.calls.add(end);
    set && set.add(end);
    return end;
};

Timeout.prototype.endWait = function timeoutEndWait(end, set) {
    this.calls.delete(end);
    set && set.delete(end);
    if (this.calls.size <= 0 && this.interval) {
        clearInterval(this.interval);
        this.interval = false;
    }
};

Timeout.prototype.startPromise = function timeoutStartPromise(params, fn, $meta, error, set) {
    if (Array.isArray($meta && $meta.timeout)) {
        return new Promise((resolve, reject) => {
            const endWait = this.startWait(waitError => {
                $meta.mtid = 'error';
                if ($meta.dispatch) {
                    $meta.dispatch(waitError, $meta);
                    resolve(false);
                } else {
                    resolve([waitError, $meta]);
                }
            }, $meta.timeout, error, set);
            Promise.resolve(params).then(fn)
                .then(result => {
                    endWait();
                    resolve(result);
                    return result;
                })
                .catch(fnError => {
                    endWait();
                    reject(fnError);
                });
        });
    } else {
        return Promise.resolve(params).then(fn);
    }
};

Timeout.prototype.startRequest = function startRequest($meta, error, onTimeout) {
    return Array.isArray($meta && $meta.timeout) && this.startWait(onTimeout, $meta.timeout, error);
};

module.exports = new Timeout();
