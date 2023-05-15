import { Logger } from '@nestjs/common';
import {
  Consumer,
  ConsumerConfig,
  ConsumerSubscribeTopics,
  Kafka,
  KafkaMessage,
} from 'kafkajs';
import { IConsumer } from './consumer.interface';

export class KafkaJsConsumer implements IConsumer {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;
  private readonly logger: Logger;

  constructor(
    private readonly topic: ConsumerSubscribeTopics,
    config: ConsumerConfig,
    broker: string,
  ) {
    this.kafka = new Kafka({ brokers: [broker] });
    this.consumer = this.kafka.consumer(config);
    this.logger = new Logger(`${topic.topics} - ${config.groupId}`);
  }

  async connect() {
    try {
      await this.consumer.connect();
    } catch (error) {
      this.logger.error(`Failed to connect to Kafka`);
      await this.connect();
    }
  }

  async consume(onMessage: (message: KafkaMessage) => Promise<void>) {
    await this.consumer.subscribe(this.topic);
    await this.consumer.run({
      eachMessage: async ({ message, partition }) => {
        this.logger.debug(`Proccessing message partition: ${partition}`);
        await onMessage(message);
      },
    });
  }

  async disconnect() {
    await this.consumer.disconnect();
  }
}
