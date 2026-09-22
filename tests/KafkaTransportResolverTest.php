<?php

declare(strict_types=1);

namespace Ufo\Component\AsyncKafkaTransport\Tests;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Ufo\Component\AsyncKafkaTransport\KafkaTransportResolver;
use Ufo\Component\TransportContracts\AsyncStampDTO;
use Ufo\Component\TransportContracts\AsyncTransportFactory;
use Ufo\Component\TransportContracts\Exceptions\TransportNotFoundException;
use Ufo\KafkaMessenger\Kafka\ClientFactory;
use Ufo\KafkaMessenger\Transport\KafkaTransport;
use Ufo\KafkaMessenger\Transport\KafkaTransportFactory;
use Ufo\KafkaMessenger\Transport\Stamp\KafkaKeyStamp;

#[CoversClass(KafkaTransportResolver::class)]
class KafkaTransportResolverTest extends TestCase
{
    public static function topicCases(): iterable
    {
        yield 'plaintext' => ['kafka://broker:9092/orders.created', 'orders.created'];
        yield 'tls' => ['kafka+ssl://broker:9093/tasks', 'tasks'];
        yield 'sasl with credentials' => ['kafka+sasl+ssl://user:pass@b-1:9098,b-2:9098/jobs', 'jobs'];
        yield 'query stays out of the topic' => ['kafka://broker:9092/jobs?group.id=svc', 'jobs'];
        yield 'no path falls back' => ['kafka://broker:9092', 'messages'];
        yield 'trailing slash falls back' => ['kafka://broker:9092/', 'messages'];
    }

    #[DataProvider('topicCases')]
    public function testTopicOfTheDsnBecomesThePartitionKey(string $dsn, string $topic): void
    {
        $stamp = self::resolver()->createAsyncStamp(new AsyncStampDTO($dsn));

        self::assertInstanceOf(KafkaKeyStamp::class, $stamp);
        self::assertSame($topic, $stamp->key);
    }

    public function testExplicitKeyWins(): void
    {
        $stamp = self::resolver()->createAsyncStamp(new AsyncStampDTO(
            'kafka://broker:9092/orders.created',
            extra: [KafkaTransportResolver::PARTITION_KEY => 'order-17'],
        ));

        self::assertSame('order-17', $stamp->key);
    }

    public function testHighPriorityChangesNothing(): void
    {
        $resolver = self::resolver();
        $dsn = 'kafka://broker:9092/orders.created';

        $ordinary = $resolver->createAsyncStamp(new AsyncStampDTO($dsn, highPriority: false));
        $priority = $resolver->createAsyncStamp(new AsyncStampDTO($dsn, highPriority: true));

        self::assertEquals($ordinary, $priority);
    }

    public function testTransportIsBuiltOncePerDsnWithoutTouchingTheBroker(): void
    {
        $resolver = self::resolver();
        $first = $resolver->getTransport('kafka://broker:9092/orders.created');
        $second = $resolver->getTransport('kafka+ssl://broker:9093/orders.created');

        self::assertInstanceOf(KafkaTransport::class, $first);
        self::assertSame($first, $resolver->getTransport('kafka://broker:9092/orders.created'));
        self::assertNotSame($first, $second);
    }

    public function testDsnWithoutATopicStillBuildsATransport(): void
    {
        self::assertInstanceOf(KafkaTransport::class, self::resolver()->getTransport('kafka://broker:9092'));
    }

    public function testBrokerlessDsnIsRefused(): void
    {
        $this->expectException(InvalidArgumentException::class);

        self::resolver()->getTransport('kafka://');
    }

    public function testRegistrationWithTheSdkFactory(): void
    {
        $resolver = self::resolver();
        $factory = new AsyncTransportFactory([$resolver]);

        self::assertSame(['kafka', 'kafka+ssl', 'kafka+sasl', 'kafka+sasl+ssl'], $resolver->getSupportSchemes());
        self::assertSame($resolver, $factory->getTransportResolver('kafka://broker:9092/orders.created'));
        self::assertSame($resolver, $factory->getTransportResolver('kafka+sasl+ssl://user:pass@broker:9098/orders.created'));

        $this->expectException(TransportNotFoundException::class);
        $factory->getTransportResolver('amqp://guest@localhost:5672/%2f/jobs');
    }

    private static function resolver(): KafkaTransportResolver
    {
        return new KafkaTransportResolver(new KafkaTransportFactory(new ClientFactory()));
    }
}
