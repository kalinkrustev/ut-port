module.exports = ({defineError, getError, fetchErrors}) => {
    if (!getError('port')) {
        const port = defineError('port', undefined, 'Port generic');

        defineError('configValidation', port, 'Port config validation:\r\n{message}');
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
        defineError('deadlock', port, 'Method {method} was recursively called, which may cause a deadlock!\nx-b3-traceid: {traceId}\nx-ut-stack: {sequence}');
        defineError('noMeta', port, '$meta not passed');
        defineError('noMetaForward', port, '$meta.forward not passed to method {method}');
        defineError('noTraceId', port, '$meta.forward[\'x-b3-traceid\'] not passed to method {method}');
    }

    return Object.assign({
        defineError,
        getError,
        fetchErrors
    }, fetchErrors('port'));
};
