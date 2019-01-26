# Python proximo client
## Consuming messages
Messages can be consumed in two modes one that sends acknowledgements automatically 
and one which requires the user to send the acknowledgements manually by calling the 
`acknoledge` function on the client. The examples below show both of these modes.

### Automatic acknowledgement
This mode works by invoking provided callback function for each message and acknowledging it, 
before processing the next one.
```python
from proximo import ProximoConsumeClient

with ProximoConsumeClient('localhost:6868', 'data-topic', 'my-consumer') as client:
    client.consume_with_callback(
        callback=lambda msg: print(f'{msg.id} - {msg.data}'),
    )
```

### Manual acknowledgement
This mode is useful when you want to process a given number of messages in a batch.
It's important to acknowledge messages after processing as not acknowledging them might
cause redeliveries and block delivery of following messages. 
```python
from proximo import ProximoConsumeClient

with ProximoConsumeClient('localhost:6868', 'data-topic', 'my-consumer') as client:
    messages = []
    for i, msg in enumerate(client.consume()):
        messages.append(msg)
        
        if len(messages) == 2:
            for j, message in enumerate(messages):
                print(f'Message number {j}: {message.id} - {message.data}')
                client.acknowledge(message)
```
