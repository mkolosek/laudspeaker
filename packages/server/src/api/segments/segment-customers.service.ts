import { Logger, Inject, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { DataSource, FindManyOptions, QueryRunner, Repository } from 'typeorm';
import { Account } from '../accounts/entities/accounts.entity';
import { CustomerDocument } from '../customers/schemas/customer.schema';
import { WINSTON_MODULE_NEST_PROVIDER } from 'nest-winston';
import { Readable } from 'node:stream';
import * as copyFrom from 'pg-copy-streams';
import { SegmentCustomers } from './entities/segment-customers.entity';
import { Segment } from './entities/segment.entity';

const LOCATION_LOCK_TIMEOUT_MS = +process.env.LOCATION_LOCK_TIMEOUT_MS;

@Injectable()
export class SegmentCustomersService {
  constructor(
    @Inject(WINSTON_MODULE_NEST_PROVIDER)
    private readonly logger: Logger,
    @InjectRepository(SegmentCustomers)
    public segmentCustomersRepository: Repository<SegmentCustomers>,
    @InjectRepository(Account)
    public accountRepository: Repository<Account>
  ) {}

  log(message, method, session, user = 'ANONYMOUS') {
    this.logger.log(
      message,
      JSON.stringify({
        class: SegmentCustomersService.name,
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
        class: SegmentCustomersService.name,
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
        class: SegmentCustomersService.name,
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
        class: SegmentCustomersService.name,
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
        class: SegmentCustomersService.name,
        method: method,
        session: session,
        user: user,
      })
    );
  }

  /**
   * Add a customer to a segment.
   *
   * @param {Account} account Associated Account
   * @param {Segment} journey Associated Journey
   * @param {CustomerDocument} customer Associated Customer
   * @param {string} session HTTP session token
   * @param {QueryRunner} [queryRunner]  Postgres Transaction
   * @returns
   */
  async create(
    segment: Segment,
    customer: CustomerDocument,
    session: string,
    account: Account,
    queryRunner?: QueryRunner
  ) {
    this.log(
      JSON.stringify({
        info: `Adding customer ${customer._id} to segment ${segment.id}`,
      }),
      this.create.name,
      session,
      account.email
    );

    const workspace = account?.teams?.[0]?.organization?.workspaces?.[0];

    if (queryRunner) {
      // Step 1: Check if customer is already enrolled in Journey; if so, throw error
      const location = await queryRunner.manager.findOne(SegmentCustomers, {
        where: {
          segment: { id: segment.id },
          workspace: { id: workspace.id },
          customerId: customer._id,
        },
      });

      if (location)
        throw new Error(
          `Customer ${customer._id} already a part of segment ${segment.id}`
        );

      // Step 2: Create new journey Location row, add time that user entered the journey
      await queryRunner.manager.save(SegmentCustomers, {
        segment: { id: segment.id },
        workspace,
        customerId: customer._id,
        segmentEntry: Date.now(),
      });
    } else {
      const location = await this.segmentCustomersRepository.findOne({
        where: {
          segment: { id: segment.id },
          workspace: { id: workspace.id },
          customerId: customer._id,
        },
      });
      if (location)
        throw new Error(
          `Customer ${customer._id} already a part of segment ${segment.id}`
        );
      await this.segmentCustomersRepository.save({
        segment: { id: segment.id },
        workspace,
        customerId: customer._id,
        segmentEntry: Date.now(),
      });
    }
  }

  async deleteFromSingleSegment(
    segment: Segment,
    customer: CustomerDocument,
    session: string,
    account: Account,
    queryRunner?: QueryRunner
  ) {
    this.log(
      JSON.stringify({
        info: `Removing customer ${customer._id} from segment ${segment.id}`,
      }),
      this.deleteFromSingleSegment.name,
      session,
      account.email
    );

    const workspace = account?.teams?.[0]?.organization?.workspaces?.[0];

    if (queryRunner) {
      // Step 1: Check if customer is already enrolled in Journey; if so, throw error
      const location = await queryRunner.manager.findOne(SegmentCustomers, {
        where: {
          segment: { id: segment.id },
          workspace: { id: workspace.id },
          customerId: customer._id,
        },
      });

      if (location)
        throw new Error(
          `Customer ${customer._id} already a part of segment ${segment.id}`
        );

      // Step 2: Create new journey Location row, add time that user entered the journey
      await queryRunner.manager.save(SegmentCustomers, {
        segment: { id: segment.id },
        workspace,
        customerId: customer._id,
        segmentEntry: Date.now(),
      });
    } else {
      const location = await this.segmentCustomersRepository.findOne({
        where: {
          segment: { id: segment.id },
          workspace: { id: workspace.id },
          customerId: customer._id,
        },
      });
      if (location)
        throw new Error(
          `Customer ${customer._id} already a part of segment ${segment.id}`
        );
      await this.segmentCustomersRepository.save({
        segment: { id: segment.id },
        workspace,
        customerId: customer._id,
        segmentEntry: Date.now(),
      });
    }
  }

  async deleteFromAllSegments(
    segment: Segment,
    customer: CustomerDocument,
    session: string,
    account: Account,
    queryRunner?: QueryRunner
  ) {
    this.log(
      JSON.stringify({
        info: `Removing customer ${customer._id} from segment ${segment.id}`,
      }),
      this.deleteFromAllSegments.name,
      session,
      account.email
    );

    const workspace = account?.teams?.[0]?.organization?.workspaces?.[0];

    if (queryRunner) {
      // Step 1: Check if customer is already enrolled in Journey; if so, throw error
      const location = await queryRunner.manager.findOne(SegmentCustomers, {
        where: {
          segment: { id: segment.id },
          workspace: { id: workspace.id },
          customerId: customer._id,
        },
      });

      if (location)
        throw new Error(
          `Customer ${customer._id} already a part of segment ${segment.id}`
        );

      // Step 2: Create new journey Location row, add time that user entered the journey
      await queryRunner.manager.save(SegmentCustomers, {
        segment: { id: segment.id },
        workspace,
        customerId: customer._id,
        segmentEntry: Date.now(),
      });
    } else {
      const location = await this.segmentCustomersRepository.findOne({
        where: {
          segment: { id: segment.id },
          workspace: { id: workspace.id },
          customerId: customer._id,
        },
      });
      if (location)
        throw new Error(
          `Customer ${customer._id} already a part of segment ${segment.id}`
        );
      await this.segmentCustomersRepository.save({
        segment: { id: segment.id },
        workspace,
        customerId: customer._id,
        segmentEntry: Date.now(),
      });
    }
  }

  async addBulk(
    segmentID: string,
    customers: string[],
    session: string,
    account: Account,
    client: any
  ): Promise<void> {
    if (!customers.length) return;
    const workspace = account?.teams?.[0]?.organization?.workspaces?.[0];
    const segmentEntry = Date.now();

    // Create a readable stream from the array od customer IDs
    const readableStream = new Readable({
      read() {
        customers.forEach((customerId) => {
          this.push(
            `${segmentID}\t${customerId}\t${workspace.id}\t${segmentEntry}\n`
          );
        });
        this.push(null);
      },
    });

    const stream = client.query(
      copyFrom.from(
        `COPY segment_customers ("segmentId", "customerId", "workspaceId", "segmentEntry") FROM STDIN WITH (FORMAT text)`
      )
    );

    // Error handling
    stream.on('error', (error) => {
      this.error(error, this.addBulk.name, session, account.email);
      throw error;
    });
    stream.on('finish', () => {
      this.debug(
        `Finished creating segment rows for ${segmentID}`,
        this.addBulk.name,
        session,
        account.email
      );
    });

    // Pipe the readable stream to the COPY command
    readableStream.pipe(stream);
  }

  async removeBulk(
    segmentID: string,
    customers: string[],
    session: string,
    account: Account,
    client: any
  ): Promise<void> {
    if (!customers.length) return;
    const workspace = account?.teams?.[0]?.organization?.workspaces?.[0];
    const segmentEntry = Date.now();

    // Create a readable stream from your customers array
    const readableStream = new Readable({
      read() {
        customers.forEach((customerId) => {
          this.push(
            `${segmentID}\t${customerId}\t${workspace.id}\t${segmentEntry}\n`
          );
        });
        this.push(null); // No more data
      },
    });

    const stream = client.query(
      copyFrom.from(
        `COPY segment_customer ("segmentId", "customerId", "workspace", "segmentEntry") FROM STDIN WITH (FORMAT text)`
      )
    );

    // Error handling
    stream.on('error', (error) => {
      this.error(error, this.addBulk.name, session, account.email);
      throw error;
    });
    stream.on('finish', () => {
      this.debug(
        `Finished creating journey location rows for ${segmentID}`,
        this.addBulk.name,
        session,
        account.email
      );
    });

    // Pipe the readable stream to the COPY command
    readableStream.pipe(stream);
  }

  /**
   * Get the number of unique customers enrolled in a specific segment
   *
   * @param account
   * @param journey
   * @param runner
   * @returns number of unique customers enrolled in a specific segment
   */
  async getNumberOfCustomersInSegment(
    account: Account,
    segment: Segment,
    runner?: QueryRunner
  ) {
    const queryCriteria: FindManyOptions<SegmentCustomers> = {
      where: {
        workspace: { id: account.teams?.[0]?.organization?.workspaces?.[0].id },
        segment: { id: segment.id },
      },
    };
    let count: number;
    if (runner) {
      count = await runner.manager.count(SegmentCustomers, queryCriteria);
    } else {
      count = await this.segmentCustomersRepository.count(queryCriteria);
    }
    return count;
  }

  /**
   * Get the number of unique customers enrolled in a specific segment
   *
   * @param account
   * @param journey
   * @param runner
   * @returns number of unique customers enrolled in a specific segment
   */
  async getSegmentsForCustomer(
    account: Account,
    segment: Segment,
    runner?: QueryRunner
  ) {
    const queryCriteria: FindManyOptions<SegmentCustomers> = {
      where: {
        workspace: { id: account.teams?.[0]?.organization?.workspaces?.[0].id },
        segment: { id: segment.id },
      },
    };
    let count: number;
    if (runner) {
      count = await runner.manager.count(SegmentCustomers, queryCriteria);
    } else {
      count = await this.segmentCustomersRepository.count(queryCriteria);
    }
    return count;
  }

  /**
   * Get the number of unique customers enrolled in a specific segment
   *
   * @param account
   * @param journey
   * @param runner
   * @returns number of unique customers enrolled in a specific segment
   */
  async getCustomersInSegment(
    account: Account,
    segment: Segment,
    runner?: QueryRunner
  ) {
    const queryCriteria: FindManyOptions<SegmentCustomers> = {
      where: {
        workspace: { id: account.teams?.[0]?.organization?.workspaces?.[0].id },
        segment: { id: segment.id },
      },
    };
    let count: number;
    if (runner) {
      count = await runner.manager.count(SegmentCustomers, queryCriteria);
    } else {
      count = await this.segmentCustomersRepository.count(queryCriteria);
    }
    return count;
  }

  async isCustomerInSegment(
    account: Account,
    segment: any,
    customer: string,
    runner?: QueryRunner
  ) {
    const queryCriteria: FindManyOptions<SegmentCustomers> = {
      where: {
        // workspace: { id: account.teams?.[0]?.organization?.workspaces?.[0].id },
        segment: segment,
        customerId: customer,
      },
    };
    let found: SegmentCustomers;
    if (runner) {
      found = await runner.manager.findOne(SegmentCustomers, queryCriteria);
    } else {
      found = await this.segmentCustomersRepository.findOne(queryCriteria);
    }
    return found ? true : false;
  }
}
