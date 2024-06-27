/* eslint-disable no-case-declarations */
import { Inject, Logger } from '@nestjs/common';
import { Injectable } from '@nestjs/common';
import { WINSTON_MODULE_NEST_PROVIDER } from 'nest-winston';
import {
  Processor,
  WorkerHost,
  InjectQueue,
  OnWorkerEvent,
} from '@nestjs/bullmq';
import { Job, MetricsTime, Queue } from 'bullmq';
import { StepType } from '../types/step.interface';
import { Step } from '../entities/step.entity';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import {
  Customer,
  CustomerDocument,
} from '@/api/customers/schemas/customer.schema';
import { Account } from '@/api/accounts/entities/accounts.entity';
import * as _ from 'lodash';
import * as Sentry from '@sentry/node';
import { JourneyLocationsService } from '@/api/journeys/journey-locations.service';
import { StepsService } from '../steps.service';
import { Journey } from '@/api/journeys/entities/journey.entity';
import { JourneyLocation } from '@/api/journeys/entities/journey-location.entity';
import { CacheService } from '@/common/services/cache.service';
import { CustomersService } from '@/api/customers/customers.service';
import { QueueService } from '@/common/services/queue.service';

@Injectable()
@Processor('{multisplit.step}', {
  stalledInterval: process.env.MULTISPLIT_STEP_PROCESSOR_STALLED_INTERVAL
    ? +process.env.MULTISPLIT_STEP_PROCESSOR_STALLED_INTERVAL
    : 600000,
  removeOnComplete: {
    age: process.env.STEP_PROCESSOR_REMOVE_ON_COMPLETE_AGE
      ? +process.env.STEP_PROCESSOR_REMOVE_ON_COMPLETE_AGE
      : 0,
    count: process.env.MULTISPLIT_STEP_PROCESSOR_REMOVE_ON_COMPLETE
      ? +process.env.MULTISPLIT_STEP_PROCESSOR_REMOVE_ON_COMPLETE
      : 0,
  },
  metrics: {
    maxDataPoints: MetricsTime.ONE_WEEK,
  },
  concurrency: process.env.MULTISPLIT_STEP_PROCESSOR_CONCURRENCY
    ? +process.env.MULTISPLIT_STEP_PROCESSOR_CONCURRENCY
    : 1,
})
export class MultisplitStepProcessor extends WorkerHost {
  constructor(
    @Inject(WINSTON_MODULE_NEST_PROVIDER)
    private readonly logger: Logger,
    @InjectQueue('{start.step}') private readonly startStepQueue: Queue,
    @InjectQueue('{wait.until.step}')
    private readonly waitUntilStepQueue: Queue,
    @InjectQueue('{message.step}') private readonly messageStepQueue: Queue,
    @InjectQueue('{jump.to.step}') private readonly jumpToStepQueue: Queue,
    @InjectQueue('{time.delay.step}')
    private readonly timeDelayStepQueue: Queue,
    @InjectQueue('{time.window.step}')
    private readonly timeWindowStepQueue: Queue,
    @InjectQueue('{multisplit.step}')
    private readonly multisplitStepQueue: Queue,
    @InjectQueue('{experiment.step}')
    private readonly experimentStepQueue: Queue,
    @InjectQueue('{exit.step}') private readonly exitStepQueue: Queue,
    @InjectModel(Customer.name) public customerModel: Model<CustomerDocument>,
    @Inject(JourneyLocationsService)
    private journeyLocationsService: JourneyLocationsService,
    @Inject(StepsService) private stepsService: StepsService,
    @Inject(CacheService) private cacheService: CacheService,
    @Inject(CustomersService) private customersService: CustomersService,
    @Inject(QueueService) private queueService: QueueService,
  ) {
    super();
  }

  log(message, method, session, user = 'ANONYMOUS') {
    this.logger.log(
      message,
      JSON.stringify({
        class: MultisplitStepProcessor.name,
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
        class: MultisplitStepProcessor.name,
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
        class: MultisplitStepProcessor.name,
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
        class: MultisplitStepProcessor.name,
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
        class: MultisplitStepProcessor.name,
        method: method,
        session: session,
        user: user,
      })
    );
  }

  async process(
    job: Job<
      {
        step: Step;
        owner: Account;
        journey: Journey;
        customer: CustomerDocument;
        location: JourneyLocation;
        session: string;
        event?: string;
        branch?: number;
        stepDepth: number;
      },
      any,
      string
    >
  ): Promise<any> {
    return Sentry.startSpan(
      { name: 'MultisplitStepProcessor.process' },
      async () => {
        let nextJob: any,
          nextStep: Step,
          nextStepId: string,
          matches = false;

        for (
          let branchIndex = 0;
          branchIndex < job.data.step.metadata.branches.length;
          branchIndex++
        ) {
          if (
            await this.customersService.checkCustomerMatchesQuery(
              job.data.step.metadata.branches[branchIndex].conditions.query,
              job.data.owner,
              job.data.session,
              job.data.customer
            )
          ) {
            matches = true;
            nextStepId =
              job.data.step.metadata.branches[branchIndex].destination;
            break;
          }
        }
        if (!matches) nextStepId = job.data.step.metadata.allOthers;

        nextStep = await this.cacheService.getIgnoreError(
          Step,
          nextStepId,
          async () => {
            return await this.stepsService.lazyFindByID(nextStepId);
          }
        );

        if (nextStep) {
          const nextStepDepth: number = this.queueService.getNextStepDepthFromJob(job);

          if (
            nextStep.type !== StepType.TIME_DELAY &&
            nextStep.type !== StepType.TIME_WINDOW &&
            nextStep.type !== StepType.WAIT_UNTIL_BRANCH
          ) {
            nextJob = {
              owner: job.data.owner,
              journey: job.data.journey,
              step: nextStep,
              session: job.data.session,
              customer: job.data.customer,
              location: job.data.location,
              event: job.data.event,
              stepDepth: nextStepDepth
            };
          } else {
            // Destination is time based,
            // customer has stopped moving so we can release lock
            await this.journeyLocationsService.unlock(
              job.data.location,
              nextStep
            );
          }
        } else {
          // Destination does not exist,
          // customer has stopped moving so we can release lock
          await this.journeyLocationsService.unlock(
            job.data.location,
            job.data.step
          );
        }

        if (nextStep && nextJob)
          await this.queueService.add(nextStep.type, nextJob);
      }
    );
  }

  @OnWorkerEvent('failed')
  async onFailed(job: Job, error: Error, prev?: string) {
    Sentry.withScope((scope) => {
      scope.setTag('job_id', job.id);
      scope.setTag('processor', MultisplitStepProcessor.name);
      Sentry.captureException(error);
    });
  }
}
