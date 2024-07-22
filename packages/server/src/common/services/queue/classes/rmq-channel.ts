import { RMQConnection } from './rmq-connection';
const amqplib = require('amqplib');

export class RMQChannel {

  public connection;
  public channel;

  constructor(connection: RMQConnection) {
    this.connection = connection;
  }

  async init() {
    this.channel = await this.connection.connection.createChannel();
  }

  async close() {
    return this.channel.close();
  }
}