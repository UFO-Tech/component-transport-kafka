<?php

declare(strict_types=1);

namespace Ufo\Component\AsyncKafkaTransport;

use Symfony\Component\Messenger\Stamp\StampInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Ufo\Component\TransportContracts\AbstractAsyncTransportResolver;
use Ufo\Component\TransportContracts\AsyncStampDTO;
use Ufo\KafkaMessenger\Kafka\Dsn;
use Ufo\KafkaMessenger\Kafka\SecurityProtocol;
use Ufo\KafkaMessenger\Kafka\TransportOption;
use Ufo\KafkaMessenger\Transport\KafkaTransportFactory;
use Ufo\KafkaMessenger\Transport\Stamp\KafkaKeyStamp;

class KafkaTransportResolver extends AbstractAsyncTransportResolver
{
    public const string PARTITION_KEY = 'key';

    private const string DEFAULT_TOPIC = 'messages';
    private const string SCHEME_SUFFIX = '://';

    public function __construct(
        KafkaTransportFactory $transportFactory,
        ?SerializerInterface $serializer = null,
    ) {
        parent::__construct($transportFactory, $serializer);
    }

    public function createAsyncStamp(AsyncStampDTO $asyncStampData): StampInterface
    {
        $key = $asyncStampData->extra[self::PARTITION_KEY] ?? $this->getTopic($asyncStampData->asyncDSN);

        return new KafkaKeyStamp((string) $key);
    }

    /** @return array<string, string> */
    protected function asyncOptions(string $dsn): array
    {
        return [TransportOption::Topic->value => $this->getTopic($dsn)];
    }

    protected function getTopic(string $dsn): string
    {
        return Dsn::parse($dsn)->topic ?? self::DEFAULT_TOPIC;
    }

    /** @return list<string> */
    public function getSupportSchemes(): array
    {
        return array_map(
            static fn (SecurityProtocol $protocol): string => substr($protocol->scheme(), 0, -\strlen(self::SCHEME_SUFFIX)),
            SecurityProtocol::cases(),
        );
    }
}
