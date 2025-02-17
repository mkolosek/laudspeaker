import {
  OnWorkerEvent,
} from '@nestjs/bullmq';
import { Job, MetricsTime, Queue, UnrecoverableError } from 'bullmq';
import { Inject, Injectable, Logger, forwardRef } from '@nestjs/common';
import { DataSource, Repository } from 'typeorm';
import mongoose, { Model } from 'mongoose';
import { InjectConnection, InjectModel } from '@nestjs/mongoose';
import { Journey } from '../../journeys/entities/journey.entity';
import { WINSTON_MODULE_NEST_PROVIDER } from 'nest-winston';
import {
  PosthogEventType,
  PosthogEventTypeDocument,
} from '../schemas/posthog-event-type.schema';
import {
  PosthogEvent,
  PosthogEventDocument,
} from '../schemas/posthog-event.schema';
import { EventDocument } from '../schemas/event.schema';
import {
  Customer,
  CustomerDocument,
} from '../../customers/schemas/customer.schema';
import * as Sentry from '@sentry/node';
import { EventType } from './events.processor';
import { InjectRepository } from '@nestjs/typeorm';
import { Account } from '../../accounts/entities/accounts.entity';
import { Workspaces } from '../../workspaces/entities/workspaces.entity';
import { EventsService } from '../events.service';
import { CacheService } from '../../../common/services/cache.service';
import { FindType } from '../../customers/enums/FindType.enum';
import { Processor } from '@/common/services/queue/decorators/processor';
import { ProcessorBase } from '@/common/services/queue/classes/processor-base';
import { QueueType } from '@/common/services/queue/types/queue-type';
import { Producer } from '@/common/services/queue/classes/producer';

export enum ProviderType {
  LAUDSPEAKER = 'laudspeaker',
  WU_ATTRIBUTE = 'wu_attribute',
  MESSAGE = 'message',
}

/**
 * EventsPreProcessor is a worker class responsible for preprocessing events.
 * For every event that comes into laudspeaker, it looks up the customer that
 * corresponds to that event (or creates that customer if they don't exist),
 * does an event fan-out for every active journey in the corresponding workspace,
 * adding a corresponding job to the EventsProcessor, and saves the event to the
 * event database.
 */
@Injectable()
@Processor('events_pre')
export class EventsPreProcessor extends ProcessorBase {
  private providerMap: Record<
    ProviderType,
    (job: Job<any, any, string>) => Promise<void>
  > = {
    [ProviderType.LAUDSPEAKER]: this.handleCustom,
    [ProviderType.MESSAGE]: this.handleMessage,
    [ProviderType.WU_ATTRIBUTE]: this.handleAttributeChange,
  };

  constructor(
    @Inject(WINSTON_MODULE_NEST_PROVIDER)
    private readonly logger: Logger,
    private dataSource: DataSource,
    @InjectConnection() private readonly connection: mongoose.Connection,
    @Inject(forwardRef(() => EventsService))
    private readonly eventsService: EventsService,
    @InjectModel(Event.name)
    private eventModel: Model<EventDocument>,
    @InjectModel(Customer.name) public customerModel: Model<CustomerDocument>,
    @InjectRepository(Journey)
    private readonly journeysRepository: Repository<Journey>,
    @Inject(CacheService) private cacheService: CacheService
  ) {
    super();
  }

  log(message, method, session, user = 'ANONYMOUS') {
    this.logger.log(
      message,
      JSON.stringify({
        class: EventsPreProcessor.name,
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
        class: EventsPreProcessor.name,
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
        class: EventsPreProcessor.name,
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
        class: EventsPreProcessor.name,
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
        class: EventsPreProcessor.name,
        method: method,
        session: session,
        user: user,
      })
    );
  }

  async process(job: Job<any, any, string>): Promise<any> {
    const fn = this.providerMap[job.name];
    const that = this;

    return Sentry.startSpan(
      { name: `EventsPreProcessor.${fn.name}` },
      async () => {
        await fn.call(that, job);
      }
    );
  }

  removeDollarSignsFromKeys(obj: any) {
    const newObj = {};
    // Iterate through each property in the object
    for (const key in obj) {
      if (obj.hasOwnProperty(key)) {
        const newKey = key.startsWith('$') ? key.substring(1) : key;

        // Recursively call the function if the property is an object
        newObj[newKey] =
          typeof obj[key] === 'object' && obj[key] !== null
            ? this.removeDollarSignsFromKeys(obj[key])
            : obj[key];
      }
    }
    return newObj;
  }

  async handleCustom(
    job: Job<
      {
        owner: Account;
        workspace: Workspaces;
        event: any;
        session: string;
      },
      any,
      any
    >
  ): Promise<any> {
    let err: any;
    try {
      //find customer associated with event or create new customer if not found
      //console.time(`handleCustom - findOrCreateCustomer ${job.data.session}`)
      const {
        customer,
        findType,
      }: { customer: CustomerDocument; findType: FindType } =
        await this.eventsService.findOrCreateCustomer(
          job.data.workspace.id,
          job.data.session,
          null,
          null,
          job.data.event
        );
      //console.timeEnd(`handleCustom - findOrCreateCustomer ${job.data.session}`)
      //get all the journeys that are active, and pipe events to each journey in case they are listening for event
      //console.time(`handleCustom - find journeys ${job.data.session}`)
      let journeys: Journey[] = await this.cacheService.get(
        'Journeys',
        job.data.workspace.id,
        async () => {
          return await this.journeysRepository.find({
            where: {
              workspace: {
                id: job.data.workspace.id,
              },
              isActive: true,
              isPaused: false,
              isStopped: false,
              isDeleted: false,
            },
          });
        }
      );

      //console.timeEnd(`handleCustom - find journeys ${job.data.session}`)
      // add event to event database for visibility
      if (job.data.event) {
        //console.time(`handleCustom - create event ${job.data.session}`)
        await this.eventModel.create([
          {
            ...this.removeDollarSignsFromKeys(job.data.event),
            workspaceId: job.data.workspace.id,
            createdAt: new Date().toISOString(),
          },
        ]);
        //console.timeEnd(`handleCustom - create event ${job.data.session}`)
      }

      // Always add jobs after committing transactions, otherwise there could be race conditions
      let eventJobs = journeys.map((journey) => ({
        //to do add here modified
        account: job.data.owner,
        //workspace: job.data.workspace,
        event: job.data.event,
        journey: {
          ...journey,
          visualLayout: {
            edges: [],
            nodes: [],
          },
          inclusionCriteria: {},
        },
        customer: customer,
        session: job.data.session,
      }));

      await Producer.addBulk(QueueType.EVENTS,
        eventJobs,
        EventType.EVENT);
      await Producer.add(QueueType.EVENTS_POST, {
        ...job.data,
        workspace: undefined,
        customer,
      }, job.data.event.event);
    } catch (e) {
      this.error(
        e,
        this.handleCustom.name,
        job.data.session,
        job.data.owner.email
      );
      err = e;
    }

    if (err?.code === 11000) {
      this.warn(
        `${JSON.stringify({
          warning: 'Attempting to insert a duplicate key!',
        })}`,
        this.handleCustom.name,
        job.data.session,
        job.data.owner?.id
      );
      throw err;
    } else if (err) {
      this.error(
        err,
        this.handleCustom.name,
        job.data.session,
        job.data.owner?.id
      );
      throw err;
    }
  }

  async handleMessage(job: Job<any, any, string>): Promise<any> {
    const transactionSession = await this.connection.startSession();
    transactionSession.startTransaction();
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    let err: any;

    try {
      const journeys = await queryRunner.manager.find(Journey, {
        where: {
          workspace: {
            id: job.data.workspaceId,
          },
          isActive: true,
          isPaused: false,
          isStopped: false,
          isDeleted: false,
        },
      });
      for (let i = 0; i < journeys.length; i++) {
        await Producer.add(QueueType.EVENTS, {
            workspaceId: job.data.workspaceId,
            message: job.data.message,
            customer: job.data.customer,
            journeyID: journeys[i].id,
          }, EventType.MESSAGE);
      }

      await transactionSession.commitTransaction();
      await queryRunner.commitTransaction();
    } catch (e) {
      await transactionSession.abortTransaction();
      await queryRunner.rollbackTransaction();
      this.error(
        e,
        this.handleMessage.name,
        job.data.session,
        job.data.accountID
      );
      err = e;
    } finally {
      await transactionSession.endSession();
      await queryRunner.release();
    }
    if (err) {
      this.error(
        err,
        this.handleMessage.name,
        job.data.session,
        job.data.accountID
      );
      throw err;
    }
  }

  async handleAttributeChange(job: Job<any, any, string>): Promise<any> {
    const transactionSession = await this.connection.startSession();
    transactionSession.startTransaction();
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    let err: any;

    try {
      const journeys = await queryRunner.manager.find(Journey, {
        where: {
          workspace: {
            id: job.data.workspaceId,
          },
          isActive: true,
          isPaused: false,
          isStopped: false,
          isDeleted: false,
        },
      });
      for (let i = 0; i < journeys.length; i++) {
        if (job.data.message.operationType === 'update') {
          await Producer.add(QueueType.EVENTS,
            {
              accountID: job.data.account.id,
              customer: job.data.message.documentKey._id,
              fields: job.data.message.updateDescription?.updatedFields,
              journeyID: journeys[i].id,
            }, EventType.ATTRIBUTE);
        }
      }

      await transactionSession.commitTransaction();
      await queryRunner.commitTransaction();
    } catch (e) {
      await transactionSession.abortTransaction();
      await queryRunner.rollbackTransaction();
      this.error(
        e,
        this.handleAttributeChange.name,
        job.data.session,
        job.data.account
      );
      err = e;
    } finally {
      await transactionSession.endSession();
      await queryRunner.release();
    }
    if (err) {
      this.error(
        err,
        this.handleAttributeChange.name,
        job.data.session,
        job.data.account.id
      );
      throw err;
    }
  }

  @OnWorkerEvent('failed')
  async onFailed(job: Job, error: Error, prev?: string) {
    Sentry.withScope((scope) => {
      scope.setTag('job_id', job.id);
      scope.setTag('processor', EventsPreProcessor.name);
      Sentry.captureException(error);
    });
  }
}
