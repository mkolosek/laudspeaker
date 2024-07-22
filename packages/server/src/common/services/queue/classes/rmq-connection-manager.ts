import { RMQConnection } from './rmq-connection';
import { RMQChannel } from './rmq-channel';

export class RMQConnectionManager {

	private _connection: RMQConnection;
	private _channel: RMQChannel;

	constructor(connection: RMQConnection, channel: RMQChannel) {
		this._connection = connection;
		this._channel = channel;
	}

  static async createConnectionAndChannel(connectionTag: string) {
    const connection = new RMQConnection(connectionTag);
    await connection.init();

    const channel = new RMQChannel(connection);
    await channel.init();

    return new RMQConnectionManager(connection, channel);
  }

  get channel() {
    return this._channel;
  }

  get connection() {
    return this._connection;
  }

  get channelObj() {
    return this.channel.channel;
  }

  get connectionObj() {
    return this.connection.connection;
  }

	async close(): Promise<void> {
    const closePromise = Promise.resolve()
      .finally( () => this.channel.close() )
      .finally( () => this.connection.close() );

    return closePromise;
	}
}