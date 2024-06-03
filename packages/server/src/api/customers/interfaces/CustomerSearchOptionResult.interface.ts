import { CustomerDocument } from '../schemas/customer.schema';
import { FindType } from '../enums/FindType.enum';

export interface CustomerSearchOptionResult {
  customer: CustomerDocument;
  findType: FindType;
}
