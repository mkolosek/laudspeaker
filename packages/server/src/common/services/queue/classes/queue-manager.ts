import { RMQConnectionManager } from './rmq-connection-manager';
import { QueueType } from '../types/queue';

export class QueueManager {
  private static connectionMgr: RMQConnectionManager;
  private static queueOptions = {
    durable: true,
    arguments: {
      maxPriority: 255
    }
  }

  static async init() {
    await this.initConnection();
    await this.initQueues();
  }

  static async assertQueue(queueName: string) {
    const channel = this.connectionMgr.channelObj;

    await channel.assertQueue(queueName, this.queueOptions);
  }

  private static async initConnection() {
    this.connectionMgr = await RMQConnectionManager.createConnectionAndChannel('QueueManager');
  }

  private static async initQueues() {
    const allQueues = Object.values(QueueType);

    for(const queue of allQueues) {
      this.assertQueue(queue);
    }
  }
}