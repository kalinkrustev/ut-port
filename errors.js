module.exports = ({defineError, getError, fetchErrors}) => {
    if (!getError('port')) {
        let PortError = defineError('port', null, 'port error', 'error');
        defineError('missingParameters', PortError, 'Missing parameters', 'error');
        defineError('missingMeta', PortError, 'Missing metadata', 'error');
        defineError('notConnected', PortError, 'No connection', 'error');
        defineError('disconnect', PortError, 'Port disconnected', 'error');
        defineError('disconnectBeforeResponse', PortError, 'Disconnect before response received', 'error');
        defineError('stream', PortError, 'Port stream error', 'error');
        defineError('timeout', PortError, 'Timeout', 'error');
        defineError('echoTimeout', PortError, 'Echo retries limit exceeded', 'error');
        defineError('unhandled', PortError, 'Unhandled port error', 'error');
        defineError('bufferOverflow', PortError, 'Message size of {size} exceeds the maximum of {max}', 'error');
        defineError('socketTimeout', PortError, 'Socket timeout', 'error');
        defineError('receiveTimeout', PortError, 'Receive timeout', 'error');
        defineError('dispatchFailure', PortError, 'Cannot dispatch message to bus', 'error');
        defineError('queueNotFound', PortError, 'Queue not found', 'error');
    }
    return fetchErrors('port');
};
