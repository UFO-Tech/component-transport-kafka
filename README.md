# UFO Asynchronous Kafka Transport

`ufo-tech/component-transport-kafka` — a library for sending messages over Kafka, built on `ufo-tech/kafka-messenger` and `ufo-tech/component-transport-contracts`.

## Requirements

- PHP >=8.3 and the `rdkafka` extension.
- `ufo-tech/kafka-messenger` ^1.0.
- `ufo-tech/component-transport-contracts` ^1.
- A Kafka broker for sending messages.

## Installation

```sh
composer require ufo-tech/component-transport-kafka:^1.0
```

Until `ufo-tech/kafka-messenger` is published on Packagist, the repository is declared in `composer.json`, so no extra setup is needed.

## Usage

```php
use Symfony\Component\Messenger\Envelope;
use Ufo\Component\AsyncKafkaTransport\KafkaTransportResolver;
use Ufo\Component\TransportContracts\AsyncStampDTO;
use Ufo\Component\TransportContracts\AsyncTransportFactory;
use Ufo\KafkaMessenger\Kafka\ClientFactory;
use Ufo\KafkaMessenger\Transport\KafkaTransportFactory;

$factory = new AsyncTransportFactory([
    new KafkaTransportResolver(new KafkaTransportFactory(new ClientFactory())),
]);

$dsn = 'kafka://localhost:9092/jobs';
$resolver = $factory->getTransportResolver($dsn);
$message = (object) ['task' => 'send-email', 'recipient' => 'user@example.com'];

$resolver->getTransport($dsn)->send(new Envelope($message, [
    $resolver->createAsyncStamp(new AsyncStampDTO($dsn)),
]));
```

You can pass a custom `SerializerInterface` implementation as the second argument to `KafkaTransportResolver`. The default is `PhpSerializer`; the message consumer must use compatible serialization and message classes.

## Transport Configuration

- Supports the `kafka`, `kafka+ssl`, `kafka+sasl` and `kafka+sasl+ssl` schemes. The scheme sets `security.protocol`, so TLS and SASL need no extra options.
- The DSN path defines the topic. Without one, `messages` is used.
- Credentials, broker list and every librdkafka property fit in the DSN: `kafka+sasl+ssl://user:pass@b-1:9098,b-2:9098/jobs?sasl.mechanisms=SCRAM-SHA-512`.
- The stamp carries the partition key. Pass `extra['key']` to keep related messages in one partition and in order; without it the topic name is used.
- `AsyncStampDTO::highPriority` has no effect: Kafka has no message priority, and every record is persistent.

## Development

```console
make up-b
make composer-i
make test
```

Tests build real transports without reaching a broker, so Kafka is not required; the `rdkafka` extension is.

## License

MIT.
