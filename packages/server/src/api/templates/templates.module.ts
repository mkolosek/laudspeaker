import { forwardRef, Module } from '@nestjs/common';
import { TemplatesService } from './templates.service';
import { TemplatesController } from './templates.controller';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Template } from './entities/template.entity';
import { MongooseModule } from '@nestjs/mongoose';
import { Customer, CustomerSchema } from '../customers/schemas/customer.schema';
import { Audience } from '../audiences/entities/audience.entity';
import { Installation } from '../slack/entities/installation.entity';
import { State } from '../slack/entities/state.entity';
import { Account } from '../accounts/entities/accounts.entity';
import {
  CustomerKeys,
  CustomerKeysSchema,
} from '../customers/schemas/customer-keys.schema';
import { CustomersModule } from '../customers/customers.module';
import { SlackModule } from '../slack/slack.module';
import { WebhooksService } from '../webhooks/webhooks.service';
import { WebhooksModule } from '../webhooks/webhooks.module';
import { ModalsModule } from '../modals/modals.module';
import { WebsocketsModule } from '../../websockets/websockets.module';
import { Step } from '../steps/entities/step.entity';
import { CacheService } from '@/common/services/cache.service';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      Account,
      Audience,
      Installation,
      State,
      Step,
      Template,
    ]),
    MongooseModule.forFeature([
      { name: Customer.name, schema: CustomerSchema },
    ]),
    MongooseModule.forFeature([
      { name: CustomerKeys.name, schema: CustomerKeysSchema },
    ]),
    forwardRef(() => CustomersModule),
    SlackModule,
    MongooseModule.forFeature([
      { name: Customer.name, schema: CustomerSchema },
    ]),
    forwardRef(() => WebhooksModule),
    ModalsModule,
    forwardRef(() => WebsocketsModule),
  ],
  providers: [TemplatesService, CacheService],
  controllers: [TemplatesController],
  exports: [TemplatesService],
})
export class TemplatesModule {}
