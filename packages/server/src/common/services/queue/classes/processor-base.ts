import { OnApplicationShutdown } from '@nestjs/common';
import { WorkOrchestrator } from './work-orchestrator';

export abstract class ProcessorBase<T extends WorkOrchestrator = WorkOrchestrator>
  implements OnApplicationShutdown
{
  private readonly _worker: T | undefined;

  get worker(): T {
    if (!this._worker) {
      throw new Error(
        '"WorkOrchestrator" has not yet been initialized.',
      );
    }

    return this._worker;
  }

  abstract process(job: any): Promise<any>;

  onApplicationShutdown(signal?: string) {
    return this._worker?.close();
  }
}