import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
} from '@nestjs/common';
import { randomUUID } from 'crypto';
import { EachMessagePayload } from 'kafkajs';
import { WINSTON_MODULE_NEST_PROVIDER } from 'nest-winston';
import { KafkaConsumerService } from '../kafka/consumer.service';
import { CustomersService } from './customers.service';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';

@Injectable()
export class CustomersConsumerService implements OnApplicationBootstrap {
  constructor(
    @Inject(WINSTON_MODULE_NEST_PROVIDER)
    private readonly logger: Logger,
    private readonly consumerService: KafkaConsumerService,
    @InjectQueue('{customer_change}')
    private readonly customerChangeQueue: Queue,
    @InjectQueue('{segment_update}')
    private readonly segmentUpdateQueue: Queue
  ) {}

  log(message, method, session, user = 'ANONYMOUS') {
    this.logger.log(
      message,
      JSON.stringify({
        class: CustomersConsumerService.name,
        method: method,
        session: session,
        user: user,
      })
    );
  }
  debug(message, method, session, user = 'ANONYMOUS') {
    this.logger.debug(
      message,
      JSON.stringify({
        class: CustomersConsumerService.name,
        method: method,
        session: session,
        user: user,
      })
    );
  }
  warn(message, method, session, user = 'ANONYMOUS') {
    this.logger.warn(
      message,
      JSON.stringify({
        class: CustomersConsumerService.name,
        method: method,
        session: session,
        user: user,
      })
    );
  }
  error(error, method, session, user = 'ANONYMOUS') {
    this.logger.error(
      error.message,
      error.stack,
      JSON.stringify({
        class: CustomersConsumerService.name,
        method: method,
        session: session,
        cause: error.cause,
        name: error.name,
        user: user,
      })
    );
  }
  verbose(message, method, session, user = 'ANONYMOUS') {
    this.logger.verbose(
      message,
      JSON.stringify({
        class: CustomersService.name,
        method: method,
        session: session,
        user: user,
      })
    );
  }

  private MONGO_DB_NAME = process.env.MONGO_DB_NAME ?? 'nest'; // Confluent cluster API secret
  private MONGO_CUSTOMERS_TABLE_NAME = 'customers'; // no other way to configure since hardcoded, make sure it matches in CustomersService.constructor
  private MONGO_CHANGE_STREAM_CONSUMER_GROUP = 'laudspeaker-customers-change';

  async onApplicationBootstrap() {
    await this.consumerService.consume(
      { topics: [`${this.MONGO_DB_NAME}.${this.MONGO_CUSTOMERS_TABLE_NAME}`] },
      {
        groupId:
          process.env.NODE_ENV === 'development'
            ? 'development-group'
            : this.MONGO_CHANGE_STREAM_CONSUMER_GROUP,
      },
      {
        eachMessage: this.handleCustomerChangeStream(),
      }
    );
  }

  private handleCustomerChangeStream(this: CustomersConsumerService) {
    return async (changeMessage: EachMessagePayload) => {
      const session = randomUUID();
      try {
        // const threshold = process.env.CUSTOMER_CHANGE_QUEUE_THRESHOLD
        //   ? +process.env.CUSTOMER_CHANGE_QUEUE_THRESHOLD
        //   : 10; // Set the threshold for maximum waiting jobs

        while (true) {
          const jobCounts = await this.segmentUpdateQueue.getJobCounts(
            'active'
          );
          const waitingJobs = jobCounts.active;

          if (waitingJobs === 0) {
            break; // Exit the loop if the number of waiting jobs is below the threshold
          }

          this.warn(
            `Waiting for the segment queue to finish processing. Current waiting jobs: ${waitingJobs}`,
            this.handleCustomerChangeStream.name,
            session
          );
          await new Promise((resolve) => setTimeout(resolve, 1000)); // Sleep for 1 second before checking again
        }
        await this.customerChangeQueue.add('change', {
          session,
          changeMessage,
        });
      } catch (err) {
        this.error(err, this.handleCustomerChangeStream.name, session);
      }
    };
  }
}
