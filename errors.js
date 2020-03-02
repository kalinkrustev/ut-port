module.exports = ({defineError, getError, fetchErrors}) => {
    if (!getError('port')) {
        const port = defineError('port', undefined, 'Port generic');

        defineError('configValidation', port, 'Config validation');
        defineError('missingParameters', port, 'Missing parameters');
        defineError('missingMeta', port, 'Missing metadata');
        defineError('notConnected', port, 'No connection');
        defineError('disconnect', port, 'Port disconnected');
        defineError('disconnectBeforeResponse', port, 'Disconnect before response received');
        defineError('stream', port, 'Port stream error');
        defineError('timeout', port, 'Timeout');
        defineError('echoTimeout', port, 'Echo retries limit exceeded');
        defineError('unhandled', port, 'Unhandled port error');
        defineError('bufferOverflow', port, 'Message size of {size} exceeds the maximum of {max}');
        defineError('socketTimeout', port, 'Socket timeout');
        defineError('receiveTimeout', port, 'Receive timeout');
        defineError('dispatchFailure', port, 'Cannot dispatch message to bus');
        defineError('queueNotFound', port, 'Queue not found');
        defineError('invalidPullStream', port, 'Invalid pull stream');
        defineError('paramsValidation', port, 'Method {method} parameters failed validation');
        defineError('resultValidation', port, 'Method {method} result failed validation');
    }

    return Object.assign({
        defineError,
        getError,
        fetchErrors
    }, fetchErrors('port'));
};
