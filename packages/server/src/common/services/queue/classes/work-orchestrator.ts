import { RMQConnectionManager } from './rmq-connection-manager';
import { QueueType } from '../types/queue';

export class WorkOrchestrator {

  private queueName: string;
  private processor;
  private connectionMgr: RMQConnectionManager;

  private running;
  private closing: Promise<void> = null;
  private closed;

  constructor(queueName: string, processor, connectionMgr: RMQConnectionManager) {
    this.queueName = queueName;
    this.processor = processor;
    this.connectionMgr = connectionMgr;
  }

  async setupListeners() {
    const queue = this.queueName;

    const channel = this.connectionMgr.channelObj;

    await channel.assertQueue(queue, {
      durable: true,
      arguments: {
        maxPriority: 255
      }
    });

    await channel.prefetch( parseInt(process.env.RMQ_QUEUE_PREFETCH_COUNT ?? '1') );

    const processor = this.processor;
    const consumerTag = this.generateConsumerTag();

    const successHandler = this.handleSuccess;
    const errorHandler = this.handleError;

    channel.consume(queue, function(msg) {
      let job = JSON.parse(msg.content.toString());

      processor.process.call(processor, job)
        .then(result => successHandler(channel, msg, queue, result))
        .catch(error => errorHandler(channel, msg, queue, error));
      
    }, {
      noAck: false,
      consumerTag: consumerTag
    });
  }

  close(): Promise<void> {
    if (this.closing) {
      return this.closing;
    }

    this.closing = (async () => {
      // this.emit('closing', 'closing queue');

      await Promise.resolve()
        .finally(() => this.connectionMgr.close())
        // .finally(() => this.emit('closed'));
      this.closed = true;
    })();

    return this.closing;
  }

  private generateConsumerTag() {
    return `${process.env.LAUDSPEAKER_PROCESS_TYPE}-${process.pid}`;
  }

  private async handleSuccess(channel, msg, queue, result) {
    channel.ack(msg);
  }

  private async handleError(channel, msg, queue, error) {

    const currentRedeliveryCount = 
      msg.properties.headers['x-delivery-count']
      ? parseInt(msg.properties.headers['x-delivery-count'])
      : 1;

    // retry every 1 second for events
    if (queue == QueueType.EVENTS) {
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // message will be requeued
      channel.nack(msg);
    } else {

      // TODO: move to failed queue if count > threthold
      msg.properties.headers['x-delivery-count'] = currentRedeliveryCount + 1;
      channel.nack(msg);
    }
  }
}