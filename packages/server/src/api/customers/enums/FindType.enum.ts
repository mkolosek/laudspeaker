// This enum describes the priorities for searching for customers
export enum FindType {
  PRIMARY_KEY = 'PRIMARY_KEY',
  MESSAGE_CHANNEL = 'MESSAGE_CHANNEL',
  CORRELATION_VALUE = 'CORRELATION_VALUE',
  OTHER_IDS = 'OTHER_IDS',
  UPSERT = 'UPSERT',
  DUPLICATE_KEY_ERROR = 'DUPLICATE_KEY_ERROR',
}
