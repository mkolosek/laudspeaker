import { Module } from '@nestjs/common';
import { JobsService } from './jobs.service';
import { JobsController } from './jobs.controller';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Job } from './entities/job.entity';
import { MongooseModule } from '@nestjs/mongoose';
import { Customer, CustomerSchema } from '../customers/schemas/customer.schema';
import { Audience } from '../audiences/entities/audience.entity';
import { State } from '../slack/entities/state.entity';
import { Account } from '../accounts/entities/accounts.entity';
import { CustomersModule } from '../customers/customers.module';
import { Workflow } from '../workflows/entities/workflow.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([Account, Audience, Job, State, Workflow]),
    MongooseModule.forFeature([
      { name: Customer.name, schema: CustomerSchema },
    ]),
    CustomersModule,
  ],
  providers: [JobsService],
  controllers: [JobsController],
  exports: [JobsService],
})
export class JobsModule {}
