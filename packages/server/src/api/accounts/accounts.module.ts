import { forwardRef, Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AccountsController } from './accounts.controller';
import { Account } from './entities/accounts.entity';
import { AccountsService } from './accounts.service';
import { AuthModule } from '../auth/auth.module';
import { CustomersModule } from '../customers/customers.module';
import { WebhooksModule } from '../webhooks/webhooks.module';
import { S3Service } from '../s3/s3.service';
import { JourneysModule } from '../journeys/journeys.module';
import { TemplatesModule } from '../templates/templates.module';
import { StepsModule } from '../steps/steps.module';
import { MongooseModule } from '@nestjs/mongoose';
import {
  CustomerKeys,
  CustomerKeysSchema,
} from '../customers/schemas/customer-keys.schema';
import { Workspaces } from '../workspaces/entities/workspaces.entity';
import { Organization } from '../organizations/entities/organization.entity';
import { OrganizationPlan } from '../organizations/entities/organization-plan.entity';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: CustomerKeys.name, schema: CustomerKeysSchema },
    ]),
    TypeOrmModule.forFeature([
      Account,
      Workspaces,
      Organization,
      OrganizationPlan,
    ]),
    forwardRef(() => AuthModule),
    forwardRef(() => CustomersModule),
    forwardRef(() => JourneysModule),
    forwardRef(() => TemplatesModule),
    forwardRef(() => StepsModule),
    WebhooksModule,
  ],
  controllers: [AccountsController],
  providers: [AccountsService, S3Service],
  exports: [AccountsService],
})
export class AccountsModule {}
