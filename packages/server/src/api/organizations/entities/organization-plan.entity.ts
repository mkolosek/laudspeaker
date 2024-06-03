import {
  BaseEntity,
  Column,
  Entity,
  JoinColumn,
  OneToOne,
  PrimaryGeneratedColumn,
} from 'typeorm';
import { Organization } from './organization.entity';

export const DEFAULT_PLAN: Partial<OrganizationPlan> = {
  planName: 'startup-may-2024',
  subscribed: false,
  activePlan: false,
  billingEmail: 'none',
  segmentLimit: 25,
  activeJourneyLimit: 10,
  messageLimit: 100000000,
  customerLimit: 100000,
  seatLimit: 3,
  workspaceLimit: 2,
};

@Entity()
export class OrganizationPlan extends BaseEntity {
  @PrimaryGeneratedColumn('uuid')
  public id: string;

  @OneToOne(() => Organization, (organization) => organization.plan, {
    onDelete: 'CASCADE',
    onUpdate: 'CASCADE',
    nullable: false,
  })
  public organization: Organization;

  @Column({ default: DEFAULT_PLAN.planName })
  planName: string;

  @Column({ default: DEFAULT_PLAN.subscribed })
  subscribed: boolean;

  @Column({ default: DEFAULT_PLAN.activePlan })
  activePlan: boolean;

  @Column({ default: DEFAULT_PLAN.billingEmail })
  billingEmail: string;

  @Column({ default: DEFAULT_PLAN.segmentLimit })
  segmentLimit: number;

  @Column({ default: DEFAULT_PLAN.activeJourneyLimit })
  activeJourneyLimit: number;

  @Column({ default: DEFAULT_PLAN.messageLimit })
  messageLimit: number;

  @Column({ default: DEFAULT_PLAN.customerLimit })
  customerLimit: number;

  @Column({ default: DEFAULT_PLAN.seatLimit })
  seatLimit: number;

  @Column({ default: DEFAULT_PLAN.workspaceLimit })
  workspaceLimit: number;
}
