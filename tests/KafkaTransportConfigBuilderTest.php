<?php

declare(strict_types=1);

namespace Ufo\Component\AsyncKafkaTransport\Tests;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Ufo\Component\AsyncKafkaTransport\KafkaTransportConfigBuilder;
use Ufo\Component\TransportContracts\AsyncTransportConfigDTO;
use Ufo\KafkaMessenger\Kafka\ClientFactory;
use Ufo\KafkaMessenger\Transport\KafkaTransport;
use Ufo\KafkaMessenger\Transport\KafkaTransportFactory;

#[CoversClass(KafkaTransportConfigBuilder::class)]
class KafkaTransportConfigBuilderTest extends TestCase
{
    public function testTypeMatchesAsyncConfig(): void
    {
        self::assertSame('kafka', (new KafkaTransportConfigBuilder())->getType());
    }

    public function testUnderscoresBecomeDashesInTopic(): void
    {
        self::assertSame(
            'rpc-async-kafka.internal-dev.v1',
            (new KafkaTransportConfigBuilder())->getQueueName('rpc_async-kafka.internal_dev.v1'),
        );
    }

    public function testTopicOutsideKafkaAlphabetIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('rpc-async-kafka.інтернал.v1');

        (new KafkaTransportConfigBuilder())->getQueueName('rpc_async-kafka.інтернал.v1');
    }

    public function testTopicAndConsumerDefaultsGoToOptions(): void
    {
        $config = (new KafkaTransportConfigBuilder())->build(
            new AsyncTransportConfigDTO('rpc_async-kafka.general.v1', '%env(resolve:RPC_KAFKA_DSN)%'),
        );

        self::assertSame('rpc_async-kafka.general.v1', $config->name);
        self::assertSame('%env(resolve:RPC_KAFKA_DSN)%', $config->dsn);
        self::assertSame([
            'topic' => 'rpc-async-kafka.general.v1',
            'kafka_conf' => [
                'group.id' => 'rpc-async-kafka.general.v1',
                'auto.offset.reset' => 'earliest',
            ],
        ], $config->options);
    }

    public function testExplicitOptionsOverrideDefaults(): void
    {
        $config = (new KafkaTransportConfigBuilder())->build(new AsyncTransportConfigDTO(
            'rpc_async-kafka.general.v1',
            'kafka://broker:9092',
            ['kafka_conf' => ['group.id' => 'svc-provider', 'client.id' => 'provider-1']],
        ));

        self::assertSame('rpc-async-kafka.general.v1', $config->options['topic']);
        self::assertSame([
            'group.id' => 'svc-provider',
            'auto.offset.reset' => 'earliest',
            'client.id' => 'provider-1',
        ], $config->options['kafka_conf']);
    }

    public function testMessengerAcceptsTheOptions(): void
    {
        $config = (new KafkaTransportConfigBuilder())->build(
            new AsyncTransportConfigDTO('rpc_async-kafka.general.v1', 'kafka://broker:9092/'),
        );

        $transport = (new KafkaTransportFactory(new ClientFactory()))
            ->createTransport($config->dsn, $config->options, new PhpSerializer());

        self::assertInstanceOf(KafkaTransport::class, $transport);
    }
}
