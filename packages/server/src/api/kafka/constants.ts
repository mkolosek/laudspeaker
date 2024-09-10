/**
 * This constants needs to match with the clickhouse table that automatically
 * reads/populates kafka. See clickhouse migrations for more details.
 */
import { ClickHouseTable } from '@/common/services/clickhouse';
export const KAFKA_TOPIC_MESSAGE_STATUS = ClickHouseTable.MESSAGE_STATUS;
