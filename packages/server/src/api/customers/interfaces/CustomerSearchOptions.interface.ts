export interface CustomerSearchOptions {
  primaryKey?: {
    name?: string;
    value?: string;
  };
  messageChannels?: Record<string, any>;
  correlationValue?: string;
}
