# Repository guidance

This package is the Kafka counterpart of `ufo-tech/component-transport-amqp`.
Use PHP >=8.3, the `Ufo\Component\AsyncKafkaTransport\` PSR-4 namespace and the
resolver contracts from `ufo-tech/component-transport-contracts`.

Keep the interfaces, abstract resolver, DTO and transport selection factory in the contracts package.
The transport itself lives in `ufo-tech/kafka-messenger`: DSN parsing, options and stamps come from
there, this package only maps the SDK contract onto it.

`ufo-tech/kafka-messenger` ^1.0 carries both Symfony lines in one tag. Which one Composer picks
follows `ufo-tech/component-transport-contracts`, so a release of the contracts that still pins
`symfony/messenger` to ^7.2 keeps the whole set on the Symfony 7 line.

Run `composer validate --strict` and `composer test` after runtime changes.
Tests build real transports without connecting, so a broker is not required; ext-rdkafka is.
Do not commit vendor, Composer lock files or PHPUnit caches. Versions come from Git tags.
