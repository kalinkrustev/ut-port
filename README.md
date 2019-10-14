# UT Port

Base class to be usually extended when creating new ports.

## Scope

- Define base API for all ports.
- Define facade API for managing multiple ports.
- Initialize logging
- Initialize metrics

## API

This exports 2 APIs:

```javascript
{
    Port,
    ports
}
```

## Port public API

```Port({bus, logFactory, config})``` - create and return a port with the API below

- ```bus``` - bus to be used by the port, saved in ```this.bus```
- ```logFactory``` - factory for creating loggers
- ```config``` - port configuration to be merged with the port default in this.config

```Port.prototype.init()``` - initializes internal variables of the port

```Port.prototype.start()``` - starts the port, so that it can process messages.
  Usually here ports start to listen or initiate network / filesystem I/O

- ```result``` - return promise so that ```start``` method of the next ports
  can be called after the returned promise resolves

```Port.prototype.ready()``` - this is called after all ports are started

- ```result``` - return promise so that ```ready``` method of the next ports
  can be called after the returned promise resolves. Usually if promise is
  returned, it will be resolved when the I/O operation initiated by the start
  method has finished.

```Port.prototype.stop()``` - stops further processing of messages, closes
connections / stops listening

```Port.prototype.disconnect(reason)``` - throws disconnect error

- ```reason``` - the reason for the disconnect error. This reason is logged in
  the error log and set as cause in the thrown error.

```Port.prototype.pull(what, context)``` - creates pull streams for the port

- ```what``` - can be one of the following:
  - falsy - the method will return an object with method ```push``` that can
  be used to add messages received from outside
  - net stream - use the stream to send and receive messages from outside
  - promise returning method - will execute the method. The method can
  communicate with external systems or just execute locally

- ```context``` - context with which the pull streams are associated

## Ports public API

```ports({bus, logFactory})``` - create and return a ports object.

Returns the API below, which is accessible through the bus in the ```ports``` namespace

- ```bus``` - bus to use when creating ports
- ```logFactory``` - logger factory to use when creating ports

```ports.get({port})``` - get port by id

- ```port``` - id of the port

```ports.fetch()``` - returns array with all the created ports

```ports.create(portsConfig, envConfig)``` - create a new port

- ```portsConfig``` - port configururation coming from the implementation.
  Can be array or single value of the following:
  - function - will call the function with envConfig as parameter, the function
  should return object to be used as configuration
  - object - will be used as configuration

  When portConfig is array, multiple ports are created, with configurations
  taken from the array.

- ```envConfig``` - port configuration coming from the environment.

  If envConfig contains a property named `port`, then the value of
  this property will be merged to the port configuration.

  If envConfig contains a property matching the port id, then the value of
  this property will also be merged to the port configuration but with a higher
  priority than the `port` property.

```ports.start()``` - start all ports

```ports.stop({port})``` - stop port

- ```port``` - id of the port to stop

```ports.move({port, x, y})``` - set UI coordinates and returns the port

- ```port``` - id of the port to set coordinates

- ```x, y``` - coordinates to set

## Port telemetry

Port telemetry includes two sets of data, which ports collect. These are
generally split in non numeric and numeric data, also know as `logs` and
`metrics`. When reporting these, the port uses a standard list of tags / fields
to annotate the date. Their names are:

- `impl` - name of the implementation or application
- `location` - name of datacenter, cluster, etc. where the microservice runs
- `hostname` - name of host where the microservice runs
- `pid` - process id of the microservice
- `env` - name of environment, usually `dev`, `test`, `uat`, `prod`, etc.
- `service` - name of microservice
- `context` - type of port, usually `sql port`, `http port`, `httpserver port`, etc.
- `name` - id of port

### Port logging

Each port initilizes its own logger instance, so that logging level
can be set per each port. By default, all ports will use `info` level
for logging. The default can be changed using the configuration key `utPort.logLevel`:

```json
{
  "utPort": {
    "logLevel": "debug"
  }
}
```

In addition to the standard tags, logging defines the following additional ones:

- `msg` - includes information in text form, which can be used for indexing and searching
- `level` - descibes the logging level and can be one of:
  - `60` - `fatal` - application has reached an unrecoverable condition
  - `50` - `error` - expected error, the application will continue to process
  further requests
  - `40` - `warn` - application is processing unexpected data and will process
  it in an inefficient way
  - `30` - `info` - standard level of logging, that is of daily use of an
  application operator
  - `20` - `debug` - more verbove level of logging, that includes additional details,
  useful for application developers, may include data on API calls, parameters
  and results
  - `10` - `trace` - most detailed level, which may include low level details like
  network traces
- `@meta` - API metadata, which includes the fields:
  - `@meta.method` - method name
  - `@meta.opcode` - optional operation code that defines the method variant to execute
  - `@meta.mtid` - describes the type of message and can be one of:
    - `event` - the application is processing an internal event
    - `request` - the application is processing an API request call
    - `error` - the application is processing an API response of type error
    - `notification` - the application is processing an API notification call
    - `convert` - the application is converting data from one format to another
  - `@meta.trace`
  - `@meta.conId`
- `error` - when logging errors, this contains additional information for the error,
  represented in the following fields:
  `error.type` - string, representing the type of error
  `error.method` - the name of the method, which resulted with error
  `error.stack` - the call stack at the time of the error
  `error.cause` - any previous errors, that caused this error

### Port metrics

Each port configures a default set of metrics, named as follows:

- `time_#`, `time_#_min`, `time_#_max` - average, minimum and maxumium stage
  execution time in milliseconds. Depending on execution stage, `#` can be one of:
  - `q` - Time spent in the incoming queue
  - `r` - Time spent in the `receive` hook
  - `e` - Time spent in the `encode` hook
  - `x` - Time spent in the `execute` hook
  - `d` - Time spent in the `decode` hook
  - `s` - Time spent in the `send` hook
  - `w` - Time spent in distpatch stage
- `time_cnt` - count of completed executions
- `count_a#`, `count_a#_min`, `count_a#_max` - current, minimum and maximum concurrent
  executions per stage. `#` can be one of:
  - `r` - Count of concurrenlty executing `receive` hooks
  - `e` - Count of concurrenlty executing `encode` hooks
  - `x` - Count of concurrenlty executing `execute` hooks
  - `d` - Count of concurrenlty executing `decode` hooks
  - `s` - Count of concurrenlty executing `send` hooks
  - `w` - Count of concurrenlty executing dispatches
- `ms` - Count of sent messages per second
- `mr` - Count of received messages per second
- `bs` - Count of sent bytes per second
- `br` - Count of received bytes per second

In addition to the standard tags, metrics define the following additional ones:

- `m` - Method name. A special method name `*` is used to aggregate metrics
  for all methods.

### Telemetry tools

Various open source tools are available to store, index and visualize
telemetry data.

#### Elasticsearch

To index logs in an optimal way, an index can be created in elasticsearch using
the following request [elastic-index-ut-http](./doc/elastic-index-ut.http)

#### Grafana

To visualize metrics, the file
[grafana-ut-metrics.json](./doc/grafana-ut-metrics.json)
can be imported in Grafana as a dashboard. It requires
that metrics are stored in Prometheus and a datasource
pointed to it exists in Grafana.
