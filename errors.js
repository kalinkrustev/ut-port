const create = require('ut-error').define;
const Port = create('port');

module.exports = {
    missingParams: create('missingParameters', Port, 'Missing parameters'),
    missingMeta: create('missingMeta', Port, 'Missing metadata'),
    notConnected: create('notConnected', Port, 'No connection'),
    disconnect: create('disconnect', Port, 'Port disconnected'),
    stream: create('stream', Port, 'Port stream error'),
    echoTimeout: create('echoTimeout', Port, 'Echo retries limit exceeded'),
    unhandled: create('unhandled', Port, 'Unhandled port error'),
    bufferOverflow: create('bufferOverflow', Port, 'Message size of {size} exceeds the maximum of {max}'),
    socketTimeout: create('socketTimeout', Port, 'Socket timeout'),
    receiveTimeout: create('receiveTimeout', Port, 'Receive timeout')
};
