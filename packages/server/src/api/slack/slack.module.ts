import { forwardRef, Module } from '@nestjs/common';
import { SlackProcessor } from './slack.processor';
import { SlackController } from './slack.controller';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Installation } from './entities/installation.entity';
import { SlackService } from './slack.service';
import { MongooseModule } from '@nestjs/mongoose';
import { CustomerSchema, Customer } from '../customers/schemas/customer.schema';
import { Account } from '../accounts/entities/accounts.entity';
import { Audience } from '../audiences/entities/audience.entity';
import { State } from './entities/state.entity';
import {
  CustomerKeys,
  CustomerKeysSchema,
} from '../customers/schemas/customer-keys.schema';
import { CustomersModule } from '../customers/customers.module';
import { WebhooksService } from '../webhooks/webhooks.service';
import { Step } from '../steps/entities/step.entity';
import { Workspaces } from '../workspaces/entities/workspaces.entity';
import { Organization } from '../organizations/entities/organization.entity';
import { OrganizationPlan } from '../organizations/entities/organization-plan.entity';

function getProvidersList() {
  let providerList: Array<any> = [SlackService, WebhooksService];

  if (process.env.LAUDSPEAKER_PROCESS_TYPE == 'QUEUE') {
    providerList = [...providerList, SlackProcessor];
  }

  return providerList;
}

@Module({
  imports: [
    TypeOrmModule.forFeature([
      Account,
      Audience,
      Installation,
      State,
      Step,
      Workspaces,
      Organization,
      OrganizationPlan,
    ]),
    MongooseModule.forFeature([
      { name: Customer.name, schema: CustomerSchema },
      { name: CustomerKeys.name, schema: CustomerKeysSchema },
    ]),
    forwardRef(() => CustomersModule),
  ],
  controllers: [SlackController],
  providers: getProvidersList(),
  exports: [SlackService],
})
export class SlackModule {}
