var create = require('ut-error').define;

var Port = create('port');

module.exports = {
    missingParams: create('missingParameters', Port, 'Missing parameters'),
    missingMeta: create('missingMeta', Port, 'Missing metadata'),
    notConnected: create('notConnected', Port, 'No connection'),
    disconnect: create('disconnect', Port, 'Port disconnected'),
    stream: create('stream', Port, 'Port stream error'),
    echoTimeout: create('echoTimeout', Port, 'Echo retries limit exceeded'),
    receiveTimeout: create('receiveTimeout', Port, 'Receive timeout')
};
