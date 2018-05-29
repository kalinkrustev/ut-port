# UT Port

Base class to be usually extended when creating new ports.

## Scope

- Define base API for all ports.
- Define facade API for managing multiple ports.

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
  this property will also be merged to the port configuration but with a higher priority than the `port` property.

```ports.start()``` - start all ports

```ports.stop({port})``` - stop port

- ```port``` - id of the port to stop

```ports.move({port, x, y})``` - set UI coordinates and returns the port

- ```port``` - id of the port to set coordinates

- ```x, y``` - coordinates to set
