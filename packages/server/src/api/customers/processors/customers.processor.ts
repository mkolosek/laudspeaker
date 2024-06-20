import { InjectQueue, Processor, WorkerHost } from '@nestjs/bullmq';
import { Job, MetricsTime, Queue } from 'bullmq';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { KEYS_TO_SKIP } from '@/utils/customer-key-name-validator';
import { WINSTON_MODULE_NEST_PROVIDER } from 'nest-winston';
import { EachMessagePayload } from 'kafkajs';
import { CustomersService } from '../customers.service';
import { JourneysService } from '@/api/journeys/journeys.service';
import { ChangeStreamDocument, DataSource } from 'typeorm';
import { AccountsService } from '@/api/accounts/accounts.service';
import { SegmentsService } from '@/api/segments/segments.service';
import { InjectConnection } from '@nestjs/mongoose';
import mongoose from 'mongoose';
import { Customer, CustomerDocument } from '../schemas/customer.schema';
import { Account } from '@/api/accounts/entities/accounts.entity';
import { ProviderType } from '@/api/events/events.preprocessor';

const containsUnskippedKeys = (updateDescription) => {
  // Combine keys from updatedFields, removedFields, and the fields of truncatedArrays
  const allKeys = Object.keys(updateDescription.updatedFields)
    .concat(updateDescription.removedFields)
    .concat(updateDescription.truncatedArrays.map((array) => array.field));

  // Check if any key is not included in KEYS_TO_SKIP, considering prefix matches
  return allKeys.some(
    (key) =>
      !KEYS_TO_SKIP.some(
        (skipKey) => key.startsWith(skipKey) || key === skipKey
      )
  );
};

const copyMessageWithFilteredUpdateDescription = (message) => {
  // Filter updatedFields with consideration for dynamic keys like journeyEnrollmentsDates.<uuid>
  const filteredUpdatedFields = Object.entries(
    message.updateDescription.updatedFields
  )
    .filter(
      ([key]) =>
        !KEYS_TO_SKIP.some(
          (skipKey) => key.startsWith(skipKey) || key === skipKey
        )
    )
    .reduce((acc, [key, value]) => {
      acc[key] = value;
      return acc;
    }, {});

  // Assume removedFields and truncatedArrays are handled as before since they're not affected by the new information
  const filteredRemovedFields = message.updateDescription.removedFields.filter(
    (key) => !KEYS_TO_SKIP.includes(key)
  );
  const filteredTruncatedArrays =
    message.updateDescription.truncatedArrays.filter(
      (entry) => !KEYS_TO_SKIP.includes(entry.field)
    );

  // Constructing a new message object with filtered updateDescription
  const newMessage = {
    ...message,
    updateDescription: {
      ...message.updateDescription,
      updatedFields: filteredUpdatedFields,
      removedFields: filteredRemovedFields,
      truncatedArrays: filteredTruncatedArrays,
    },
  };

  return newMessage;
};

@Injectable()
@Processor('{customer_change}', {
  stalledInterval: process.env.CUSTOMER_CHANGE_PROCESSOR_STALLED_INTERVAL
    ? +process.env.CUSTOMER_CHANGE_PROCESSOR_STALLED_INTERVAL
    : 30000,
  removeOnComplete: {
    age: process.env.STEP_PROCESSOR_REMOVE_ON_COMPLETE_AGE
      ? +process.env.STEP_PROCESSOR_REMOVE_ON_COMPLETE_AGE
      : 0,
    count: process.env.CUSTOMER_CHANGE_PROCESSOR_REMOVE_ON_COMPLETE
      ? +process.env.CUSTOMER_CHANGE_PROCESSOR_REMOVE_ON_COMPLETE
      : 0,
  },
  metrics: {
    maxDataPoints: MetricsTime.ONE_WEEK,
  },
  concurrency: process.env.CUSTOMER_CHANGE_PROCESSOR_CONCURRENCY
    ? +process.env.CUSTOMER_CHANGE_PROCESSOR_CONCURRENCY
    : 1,
})
export class CustomerChangeProcessor extends WorkerHost {
  constructor(
    @Inject(WINSTON_MODULE_NEST_PROVIDER)
    private readonly logger: Logger,
    private readonly customersService: CustomersService,
    private readonly journeysService: JourneysService,
    private readonly accountsService: AccountsService,
    private readonly segmentsService: SegmentsService,
    @InjectQueue('{events_pre}')
    private readonly eventPreprocessorQueue: Queue,
    @InjectConnection() private readonly connection: mongoose.Connection,
    private dataSource: DataSource
  ) {
    super();
  }

  log(message, method, session, user = 'ANONYMOUS') {
    this.logger.log(
      message,
      JSON.stringify({
        class: CustomerChangeProcessor.name,
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
        class: CustomerChangeProcessor.name,
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
        class: CustomerChangeProcessor.name,
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
        class: CustomerChangeProcessor.name,
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
        class: CustomerChangeProcessor.name,
        method: method,
        session: session,
        user: user,
      })
    );
  }

  async process(
    job: Job<
      { session: string; changeMessage: EachMessagePayload },
      any,
      string
    >
  ): Promise<any> {
    let err: any;
    const queryRunner = await this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();
    const clientSession = await this.connection.startSession();
    await clientSession.startTransaction();
    try {
      let message: ChangeStreamDocument<Customer> | String = Buffer.from(
        job.data.changeMessage.message.value
      ).toString();

      // keep parsing until the kafka payload is turned into an object
      while (typeof message === 'string' || message instanceof String) {
        message = JSON.parse(message.toString());
      }

      let account: Account;
      let customer: CustomerDocument;
      switch (message.operationType) {
        case 'insert':
        case 'update':
        case 'replace':
          if (
            message.operationType === 'update' &&
            !containsUnskippedKeys(message.updateDescription)
          )
            break;
          customer = await this.customersService.findByCustomerId(
            message.documentKey._id,
            clientSession
          );
          if (!customer) {
            this.warn(
              `No customer with id ${message.documentKey._id}`,
              this.process.name,
              job.data.session
            );
            break;
          }
          account =
            await this.accountsService.findOrganizationOwnerByWorkspaceId(
              customer.workspaceId,
              job.data.session
            );
          await this.segmentsService.updateCustomerSegments(
            account,
            customer._id,
            job.data.session,
            queryRunner
          );
          await this.journeysService.updateEnrollmentForCustomer(
            account,
            customer._id,
            message.operationType === 'insert' ? 'NEW' : 'CHANGE',
            job.data.session,
            queryRunner,
            clientSession
          );
          if (message.operationType === 'update')
            await this.eventPreprocessorQueue.add(ProviderType.WU_ATTRIBUTE, {
              account: account,
              session: job.data.session,
              message: copyMessageWithFilteredUpdateDescription(message),
            });
          break;
        case 'delete': {
          // TODO_JH: remove customerID from all steps also
          const customerId = message.documentKey._id;
          await this.segmentsService.removeCustomerFromAllSegments(
            customerId,
            queryRunner
          );
          break;
        }
      }
      await clientSession.commitTransaction();
      await queryRunner.commitTransaction();
    } catch (e) {
      this.error(e, this.process.name, job.data.session);
      err = e;
      await clientSession.abortTransaction();
      await queryRunner.rollbackTransaction();
    } finally {
      await clientSession.endSession();
      await queryRunner.release();
      if (err) throw err;
    }
  }
}
