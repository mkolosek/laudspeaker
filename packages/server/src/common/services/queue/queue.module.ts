import { DynamicModule, Module } from '@nestjs/common';
import { QueueExplorer } from './queue.explorer';
import { DiscoveryModule } from '@nestjs/core';

@Module({})
export class QueueModule {
  static forRoot(options: Record<string, any>): DynamicModule {
    // const connection = await this.connect();

    // const queueProviders = createQueueConsumerProviders(optionsArr);

    return {
      module: QueueModule,
      imports: [DiscoveryModule],
      providers: [QueueExplorer],
    };
  }
}