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

### Port({bus, logFactory, config})

### Port.prototype.init()

### Port.prototype.start()

### Port.prototype.ready()

### Port.prototype.stop()

### Port.prototype.disconnect(reason)

### Port.prototype.pipe(stream, context)

### Port.prototype.pipeReverse(stream, context)

### Port.prototype.pipeExec(exec)

## ports public API

### ports({bus, logFactory})

### ports.get()

### ports.fetch()

### ports.create()

### ports.start()

### ports.stop()

### ports.move()
