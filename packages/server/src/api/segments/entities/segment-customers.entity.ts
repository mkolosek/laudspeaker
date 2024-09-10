import {
  BaseEntity,
  Column,
  Entity,
  JoinColumn,
  ManyToOne,
  PrimaryColumn,
} from 'typeorm';
import { Segment } from './segment.entity';
import { Workspaces } from '../../workspaces/entities/workspaces.entity';

@Entity()
export class SegmentCustomers extends BaseEntity {
  @PrimaryColumn({ name: 'segmentId' })
  @JoinColumn({ name: 'segmentId' })
  @ManyToOne(() => Segment, (segment) => segment.id, { onDelete: 'CASCADE' })
  public segment: Segment;

  @PrimaryColumn()
  public customerId: string;

  @JoinColumn()
  @ManyToOne(() => Workspaces, (workspace) => workspace.id, {
    onDelete: 'CASCADE',
  })
  workspace: Workspaces;

  // This is actually a timestamp using ECMAScript's native Date object; will yield
  // the same number across any timezone
  @Column({ type: 'bigint', nullable: true, default: 0 })
  segmentEntry: number;
}
