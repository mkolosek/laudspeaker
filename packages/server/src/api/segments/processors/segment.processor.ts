import { Inject, Logger, forwardRef } from '@nestjs/common';
import { Injectable } from '@nestjs/common';
import { WINSTON_MODULE_NEST_PROVIDER } from 'nest-winston';
import {
  Processor,
  WorkerHost,
  OnWorkerEvent,
  InjectQueue,
} from '@nestjs/bullmq';
import { Job, MetricsTime, Queue } from 'bullmq';
import { DataSource, Repository } from 'typeorm';
import * as _ from 'lodash';
import * as Sentry from '@sentry/node';
import { Account } from '../../accounts/entities/accounts.entity';
import { Segment, SegmentType } from '../entities/segment.entity';
import { SegmentsService } from '../segments.service';
import { CustomersService } from '../../customers/customers.service';
import { CreateSegmentDTO } from '../dto/create-segment.dto';
import { InjectConnection } from '@nestjs/mongoose';
import mongoose from 'mongoose';
import { SegmentCustomers } from '../entities/segment-customers.entity';
import { InjectRepository } from '@nestjs/typeorm';
import { UpdateSegmentDTO } from '../dto/update-segment.dto';
import { Workspaces } from '../../workspaces/entities/workspaces.entity';
import { SegmentCustomersService } from '../segment-customers.service';
import { Journey } from '../../journeys/entities/journey.entity';
import { Step } from '../../steps/entities/step.entity';
import { StepsService } from '@/api/steps/steps.service';
import { StepType } from '@/api/steps/types/step.interface';

@Injectable()
@Processor('{segment_update}', {
  stalledInterval: process.env.SEGMENT_UPDATE_PROCESSOR_STALLED_INTERVAL
    ? +process.env.SEGMENT_UPDATE_PROCESSOR_STALLED_INTERVAL
    : 600000,
  removeOnComplete: {
    age: process.env.SEGMENT_UPDATE_PROCESSOR_REMOVE_ON_COMPLETE_AGE
      ? +process.env.SEGMENT_UPDATE_PROCESSOR_REMOVE_ON_COMPLETE_AGE
      : 0,
    count: process.env.SEGMENT_UPDATE_PROCESSOR_REMOVE_ON_COMPLETE
      ? +process.env.SEGMENT_UPDATE_PROCESSOR_REMOVE_ON_COMPLETE
      : 0,
  },
  metrics: {
    maxDataPoints: MetricsTime.ONE_WEEK,
  },
  concurrency: process.env.SEGMENT_UPDATE_PROCESSOR_CONCURRENCY
    ? +process.env.SEGMENT_UPDATE_PROCESSOR_CONCURRENCY
    : 1,
})
export class SegmentUpdateProcessor extends WorkerHost {
  private providerMap = {
    updateDynamic: this.handleUpdateDynamic,
    updateManual: this.handleUpdateManual,
    createDynamic: this.handleCreateDynamic,
    createSystem: this.handleCreateSystem,
  };
  constructor(
    private dataSource: DataSource,
    @Inject(WINSTON_MODULE_NEST_PROVIDER)
    private readonly logger: Logger,
    @Inject(SegmentsService) private segmentsService: SegmentsService,
    @Inject(StepsService) private stepsService: StepsService,
    @Inject(forwardRef(() => CustomersService))
    private customersService: CustomersService,
    @InjectConnection() private readonly connection: mongoose.Connection,
    @InjectQueue('{customer_change}')
    private readonly customerChangeQueue: Queue,
    @InjectQueue('{enrollment}')
    private readonly enrollmentQueue: Queue,
    @InjectQueue('{imports}') private readonly importsQueue: Queue,
    @Inject(SegmentCustomersService)
    private segmentCustomersService: SegmentCustomersService,
    @InjectRepository(SegmentCustomers)
    private segmentCustomersRepository: Repository<SegmentCustomers>
  ) {
    super();
  }

  log(message, method, session, user = 'ANONYMOUS') {
    this.logger.log(
      message,
      JSON.stringify({
        class: SegmentUpdateProcessor.name,
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
        class: SegmentUpdateProcessor.name,
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
        class: SegmentUpdateProcessor.name,
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
        class: SegmentUpdateProcessor.name,
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
        class: SegmentUpdateProcessor.name,
        method: method,
        session: session,
        user: user,
      })
    );
  }

  /*
   *
   *
   */
  async process(job: any): Promise<any> {
    const fn = this.providerMap[job.name];
    const that = this;

    return Sentry.startSpan(
      { name: `${SegmentUpdateProcessor.name}.${fn.name}` },
      async () => {
        await fn.call(that, job);
      }
    );
  }

  /*
   * Creates system segments when a journey is started. For every multisplit branch, it creates a segment that
   * has the multisplit branch defintiont as the segment defintoin. All of the matching customers are then
   * added to that segment for future multisplit assessments.
   */
  async handleCreateSystem(
    job: Job<
      {
        account: Account;
        session: string;
        journey: Journey;
      },
      any,
      string
    >
  ) {
    while (true) {
      const jobCounts = await this.customerChangeQueue.getJobCounts('active');
      const activeJobs = jobCounts.active;

      if (jobCounts && jobCounts.active && jobCounts.active > 0) {
        this.warn(
          `Waiting for the customer change queue to clear. Current active jobs: ${activeJobs}`,
          this.process.name,
          job.data.session
        );
        await new Promise((resolve) => setTimeout(resolve, 1000)); // Sleep for 1 second before checking again
      } else {
        break;
      }
    }
    const queryRunner = await this.dataSource.createQueryRunner();
    const client = await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const steps: Step[] =
        await this.stepsService.transactionalfindAllByTypeInJourney(
          job.data.account,
          StepType.MULTISPLIT,
          job.data.journey.id,
          queryRunner,
          job.data.session
        );
      if (steps.length) {
        for (let stepIndex = 0; stepIndex < steps.length; stepIndex++) {
          for (
            let branchIndex = 0;
            branchIndex < steps[stepIndex].metadata.branches.length;
            branchIndex++
          ) {
            // extract the rules for each multisplit
            const segment = await queryRunner.manager.save(Segment, {
              type: SegmentType.SYSTEM,
              name: '__SYSTEM__',
              inclusionCriteria:
                steps[stepIndex].metadata.branches[branchIndex].conditions,
              workspace: {
                id: job.data.account.teams?.[0]?.organization.workspaces?.[0]
                  .id,
              },
              isUpdating: false,
            });

            steps[stepIndex].metadata.branches[branchIndex].systemSegment =
              segment.id;

            await queryRunner.manager.save(Step, steps[stepIndex]);

            const collectionPrefix =
              this.segmentsService.generateRandomString();
            const customersInSegment =
              await this.customersService.getSegmentCustomersFromQuery(
                segment.inclusionCriteria.query,
                job.data.account,
                job.data.session,
                true,
                0,
                collectionPrefix
              );

            if (!customersInSegment) continue; // The segment definition doesnt have any customers in it...
            const CUSTOMERS_PER_BATCH = 50000;
            let batch = 0;
            const mongoCollection =
              this.connection.db.collection(customersInSegment);
            const totalDocuments = await mongoCollection.countDocuments();

            while (batch * CUSTOMERS_PER_BATCH <= totalDocuments) {
              const customers = await this.customersService.find(
                job.data.account,
                segment.inclusionCriteria,
                job.data.session,
                null,
                batch * CUSTOMERS_PER_BATCH,
                CUSTOMERS_PER_BATCH,
                customersInSegment
              );
              this.log(
                `Skip ${
                  batch * CUSTOMERS_PER_BATCH
                }, limit: ${CUSTOMERS_PER_BATCH}`,
                this.handleCreateDynamic.name,
                job.data.session
              );
              batch++;

              await this.segmentCustomersService.addBulk(
                segment.id,
                customers.map((document) => {
                  return document._id.toString();
                }),
                job.data.session,
                job.data.account,
                client
              );
            }
            try {
              await this.segmentsService.deleteCollectionsWithPrefix(
                collectionPrefix
              );
            } catch (e) {
              this.error(
                e,
                this.process.name,
                job.data.session,
                job.data.account.email
              );
            }
          }
        }
      }
      await queryRunner.commitTransaction();
      await this.enrollmentQueue.add('enroll', {
        account: job.data.account,
        journey: job.data.journey,
        session: job.data.session,
      });
    } catch (err) {
      this.error(
        err,
        this.handleCreateSystem.name,
        job.data.session,
        job.data.account.email
      );
      await queryRunner.rollbackTransaction();
    } finally {
      await queryRunner.release();
    }
  }

  async handleCreateDynamic(
    job: Job<
      {
        account: Account;
        segment: Segment;
        session: string;
        createSegmentDTO: CreateSegmentDTO;
      },
      any,
      string
    >
  ) {
    let err: any;
    await this.customerChangeQueue.pause();
    while (true) {
      const jobCounts = await this.customerChangeQueue.getJobCounts('active');
      const activeJobs = jobCounts.active;

      if (activeJobs === 0) {
        break; // Exit the loop if the number of waiting jobs is below the threshold
      }

      this.warn(
        `Waiting for the customer change queue to clear. Current active jobs: ${activeJobs}`,
        this.process.name,
        job.data.session
      );
      await new Promise((resolve) => setTimeout(resolve, 1000)); // Sleep for 1 second before checking again
    }

    await this.importsQueue.pause();
    while (true) {
      const jobCounts = await this.importsQueue.getJobCounts('active');
      const activeJobs = jobCounts.active;

      if (activeJobs === 0) {
        break; // Exit the loop if the number of waiting jobs is below the threshold
      }

      this.warn(
        `Waiting for the import queue to clear. Current active jobs: ${activeJobs}`,
        this.process.name,
        job.data.session
      );
      await new Promise((resolve) => setTimeout(resolve, 1000)); // Sleep for 1 second before checking again
    }

    const queryRunner = await this.dataSource.createQueryRunner();
    const client = await queryRunner.connect();
    await queryRunner.startTransaction();
    try {
      const collectionPrefix = this.segmentsService.generateRandomString();
      const customersInSegment =
        await this.customersService.getSegmentCustomersFromQuery(
          job.data.createSegmentDTO.inclusionCriteria.query,
          job.data.account,
          job.data.session,
          true,
          0,
          collectionPrefix
        );

      const CUSTOMERS_PER_BATCH = 50000;
      let batch = 0;
      const mongoCollection = this.connection.db.collection(customersInSegment);
      const totalDocuments = await mongoCollection.countDocuments();

      while (batch * CUSTOMERS_PER_BATCH <= totalDocuments) {
        const customers = await this.customersService.find(
          job.data.account,
          job.data.createSegmentDTO.inclusionCriteria,
          job.data.session,
          null,
          batch * CUSTOMERS_PER_BATCH,
          CUSTOMERS_PER_BATCH,
          customersInSegment
        );
        this.log(
          `Skip ${batch * CUSTOMERS_PER_BATCH}, limit: ${CUSTOMERS_PER_BATCH}`,
          this.handleCreateDynamic.name,
          job.data.session
        );
        batch++;

        await this.segmentCustomersService.addBulk(
          job.data.segment.id,
          customers.map((document) => {
            return document._id.toString();
          }),
          job.data.session,
          job.data.account,
          client
        );
      }
      try {
        await this.segmentsService.deleteCollectionsWithPrefix(
          collectionPrefix
        );
      } catch (e) {
        this.error(
          e,
          this.process.name,
          job.data.session,
          job.data.account.email
        );
      }

      await queryRunner.manager.save(Segment, {
        ...job.data.segment,
        isUpdating: false,
      });
      await queryRunner.commitTransaction();
    } catch (e) {
      this.error(
        e,
        this.process.name,
        job.data.session,
        job.data.account.email
      );
      await queryRunner.rollbackTransaction();
      err = e;
    } finally {
      await queryRunner.release();
      await this.customerChangeQueue.resume();
      await this.importsQueue.resume();
      if (err) throw err;
    }
  }

  async handleUpdateDynamic(
    job: Job<
      {
        account: Account;
        id: string;
        session: string;
        updateSegmentDTO: UpdateSegmentDTO;
        workspace: Workspaces;
      },
      any,
      string
    >
  ) {
    let err: any;
    await this.customerChangeQueue.pause();
    while (true) {
      const jobCounts = await this.customerChangeQueue.getJobCounts('active');
      const activeJobs = jobCounts.active;

      if (activeJobs === 0) {
        break; // Exit the loop if the number of waiting jobs is below the threshold
      }

      this.warn(
        `Waiting for the queue to clear. Current active jobs: ${activeJobs}`,
        this.process.name,
        job.data.session
      );
      await new Promise((resolve) => setTimeout(resolve, 1000)); // Sleep for 1 second before checking again
    }
    const queryRunner = await this.dataSource.createQueryRunner();
    const client = await queryRunner.connect();
    await queryRunner.startTransaction();
    try {
      const segment = await this.segmentsService.findOne(
        job.data.account,
        job.data.id,
        job.data.session
      );
      const forDelete = await this.segmentCustomersRepository.findBy({
        segment: { id: segment.id },
      });

      for (const { customerId } of forDelete) {
        const customer = await this.customersService.CustomerModel.findById(
          customerId
        ).exec();
        await this.segmentsService.updateAutomaticSegmentCustomerInclusion(
          job.data.account,
          customer,
          job.data.session
        );
        await this.customersService.recheckDynamicInclusion(
          job.data.account,
          customer,
          job.data.session
        );
      }

      const amount = await this.customersService.CustomerModel.count({
        workspaceId: job.data.workspace.id,
      });

      const batchOptions = {
        current: 0,
        documentsCount: amount || 0,
        batchSize: 500,
      };

      while (batchOptions.current < batchOptions.documentsCount) {
        const batch = await this.customersService.CustomerModel.find({
          workspaceId: job.data.workspace.id,
        })
          .skip(batchOptions.current)
          .limit(batchOptions.batchSize)
          .exec();

        for (const customer of batch) {
          await this.segmentsService.updateAutomaticSegmentCustomerInclusion(
            job.data.account,
            customer,
            job.data.session
          );
        }

        batchOptions.current += batchOptions.batchSize;
      }

      const records = await this.segmentCustomersRepository.findBy({
        segment: { id: segment.id },
      });

      for (const { customerId } of records) {
        const customer = await this.customersService.CustomerModel.findById(
          customerId
        ).exec();
        await this.customersService.recheckDynamicInclusion(
          job.data.account,
          customer,
          job.data.session
        );
      }

      await queryRunner.manager.save(Segment, {
        ...segment,
        isUpdating: false,
      });
      await queryRunner.commitTransaction();
    } catch (e) {
      this.error(
        e,
        this.process.name,
        job.data.session,
        job.data.account.email
      );
      await queryRunner.rollbackTransaction();
      err = e;
    } finally {
      await queryRunner.release();
      await this.customerChangeQueue.resume();
      if (err) throw err;
    }
  }

  async handleUpdateManual(
    job: Job<
      {
        account: Account;
        segment: Segment;
        session: string;
        csvFile: any;
      },
      any,
      string
    >
  ) {
    let err: any;
    const queryRunner = await this.dataSource.createQueryRunner();
    const client = await queryRunner.connect();
    await queryRunner.startTransaction();
    try {
      const { stats } = await this.customersService.loadCSV(
        job.data.account,
        job.data.csvFile,
        job.data.session
      );

      await this.segmentsService.assignCustomers(
        job.data.account,
        job.data.segment.id,
        stats.customers,
        job.data.session
      );

      await queryRunner.manager.save(Segment, {
        ...job.data.segment,
        isUpdating: false,
      });
      await queryRunner.commitTransaction();
    } catch (e) {
      this.error(
        e,
        this.handleUpdateManual.name,
        job.data.session,
        job.data.account.email
      );
      await queryRunner.rollbackTransaction();
      err = e;
    } finally {
      await queryRunner.release();
      if (err) throw err;
    }
  }

  @OnWorkerEvent('failed')
  async onFailed(job: Job, error: Error, prev?: string) {
    Sentry.withScope((scope) => {
      scope.setTag('job_id', job.id);
      scope.setTag('processor', SegmentUpdateProcessor.name);
      Sentry.captureException(error);
    });
  }
}
