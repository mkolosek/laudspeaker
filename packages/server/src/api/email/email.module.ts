import { Module } from '@nestjs/common';
import { EmailController } from './email.controller';
import { MessageProcessor } from './email.processor';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Account } from '../accounts/entities/accounts.entity';
import { Audience } from '../audiences/entities/audience.entity';
import { MongooseModule } from '@nestjs/mongoose';
import { Customer, CustomerSchema } from '../customers/schemas/customer.schema';
import {
  CustomerKeys,
  CustomerKeysSchema,
} from '../customers/schemas/customer-keys.schema';
import { CustomersModule } from '../customers/customers.module';
import { WebhooksModule } from '../webhooks/webhooks.module';

function getProvidersList() {
  let providerList: Array<any> = [];

  if (process.env.LAUDSPEAKER_PROCESS_TYPE == 'QUEUE') {
    providerList = [...providerList, MessageProcessor];
  }

  return providerList;
}

@Module({
  imports: [
    TypeOrmModule.forFeature([Account, Audience]),
    MongooseModule.forFeature([
      { name: Customer.name, schema: CustomerSchema },
    ]),
    MongooseModule.forFeature([
      { name: CustomerKeys.name, schema: CustomerKeysSchema },
    ]),
    CustomersModule,
    WebhooksModule,
  ],
  controllers: [EmailController],
  providers: getProvidersList(),
})
export class EmailModule {}
