import { InjectQueue, Processor, WorkerHost } from '@nestjs/bullmq';
import { Job, MetricsTime, Queue } from 'bullmq';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { WINSTON_MODULE_NEST_PROVIDER } from 'nest-winston';
import { CustomersService } from '../../customers/customers.service';
import { JourneysService } from '../../journeys/journeys.service';
import { DataSource } from 'typeorm';
import { AccountsService } from '@/api/accounts/accounts.service';
import { SegmentsService } from '@/api/segments/segments.service';
import { InjectConnection } from '@nestjs/mongoose';
import mongoose from 'mongoose';
import { CustomerDocument } from '../../customers/schemas/customer.schema';
import { Account } from '../../accounts/entities/accounts.entity';
import { Workspaces } from '../../workspaces/entities/workspaces.entity';

@Injectable()
@Processor('{events_post}', {
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
export class EventsPostProcessor extends WorkerHost {
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
        class: EventsPostProcessor.name,
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
        class: EventsPostProcessor.name,
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
        class: EventsPostProcessor.name,
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
        class: EventsPostProcessor.name,
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
        class: EventsPostProcessor.name,
        method: method,
        session: session,
        user: user,
      })
    );
  }

  async process(
    job: Job<
      {
        owner: Account;
        workspace: Workspaces;
        event: any;
        session: string;
        customer: CustomerDocument;
      },
      any,
      any
    >
  ): Promise<any> {
    let err: any;
    const queryRunner = await this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();
    try {
      await this.segmentsService.updateCustomerSegmentsUsingEvent(
        job.data.owner,
        job.data.event,
        job.data.customer._id,
        job.data.session,
        queryRunner
      );
      // TODO: Add back journey enrollment updater
      // await this.journeysService.updateEnrollmentForCustomer(
      //   job.data.owner,
      //   customer._id,
      //   message.operationType === 'insert' ? 'NEW' : 'CHANGE',
      //   job.data.session,
      //   queryRunner,
      //   clientSession
      // );
      await queryRunner.commitTransaction();
    } catch (e) {
      this.error(e, this.process.name, job.data.session);
      err = e;
      await queryRunner.rollbackTransaction();
    } finally {
      await queryRunner.release();
      if (err) throw err;
    }
  }
}
