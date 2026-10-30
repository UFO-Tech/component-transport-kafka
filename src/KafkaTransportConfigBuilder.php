<?php

declare(strict_types=1);

namespace Ufo\Component\AsyncKafkaTransport;

use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Ufo\Component\TransportContracts\AsyncTransportConfigBuilderInterface;
use Ufo\Component\TransportContracts\AsyncTransportConfigDTO;
use Ufo\KafkaMessenger\Kafka\ConfProperty;
use Ufo\KafkaMessenger\Kafka\TransportOption;

/** Топік з іменем транспорту; DSN має бути без шляху, бо шлях перекриває options.topic. */
class KafkaTransportConfigBuilder implements AsyncTransportConfigBuilderInterface
{
    public const string TYPE = 'kafka';

    public const string AUTO_OFFSET_RESET = 'auto.offset.reset';
    public const string DEFAULT_AUTO_OFFSET_RESET = 'earliest';

    private const string TOPIC_PATTERN = '/^[a-zA-Z0-9._-]{1,249}$/';

    public function getType(): string
    {
        return static::TYPE;
    }

    /** @throws InvalidArgumentException */
    public function getQueueName(string $transportName): string
    {
        $topic = str_replace('_', '-', $transportName);
        if (!preg_match(self::TOPIC_PATTERN, $topic)) {
            throw new InvalidArgumentException(\sprintf('Kafka topic "%s" of transport "%s" may contain only [a-zA-Z0-9._-], up to 249 characters', $topic, $transportName));
        }

        return $topic;
    }

    /**
     * Група споживачів за замовчуванням — сам топік: воркери однієї пари ділять повідомлення.
     *
     * @throws InvalidArgumentException
     */
    public function build(AsyncTransportConfigDTO $config): AsyncTransportConfigDTO
    {
        $topic = $this->getQueueName($config->name);

        return $config->applyTransportConfig([
            TransportOption::Topic->value => $topic,
            TransportOption::KafkaConf->value => [
                ConfProperty::GroupId->value => $topic,
                self::AUTO_OFFSET_RESET => self::DEFAULT_AUTO_OFFSET_RESET,
            ],
        ]);
    }
}
