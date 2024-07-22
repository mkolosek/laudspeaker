import { Scope, SetMetadata } from '@nestjs/common';
import { PROCESSOR_METADATA } from '../queue.constants';

export function Processor(queueName: string): ClassDecorator {
  const options = {
  	name: queueName
  };

  return (target: Function) => {
    SetMetadata(PROCESSOR_METADATA, options)(target);
  };
}