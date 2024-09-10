import { MigrationInterface, QueryRunner } from 'typeorm';

export class PlanAndBilling1717146469104 implements MigrationInterface {
  name = 'PlanAndBilling1717146469104';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TABLE "organization_plan" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "planName" character varying NOT NULL DEFAULT 'startup-may-2024', "subscribed" boolean NOT NULL DEFAULT false, "activePlan" boolean NOT NULL DEFAULT false, "billingEmail" character varying NOT NULL DEFAULT 'none', "segmentLimit" integer NOT NULL DEFAULT '25', "activeJourneyLimit" integer NOT NULL DEFAULT '10', "messageLimit" integer NOT NULL DEFAULT '100000000', "customerLimit" integer NOT NULL DEFAULT '100000', "seatLimit" integer NOT NULL DEFAULT '3', "workspaceLimit" integer NOT NULL DEFAULT '2', CONSTRAINT "PK_5057d4c67d87c89f2fa8d70196b" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`ALTER TABLE "organization" ADD "planId" uuid`);
    await queryRunner.query(
      `ALTER TABLE "journey_location" ALTER COLUMN "stepEntryAt" SET NOT NULL`
    );
    await queryRunner.query(
      `ALTER TABLE "journey_location" ALTER COLUMN "stepEntryAt" SET DEFAULT now()`
    );
    await queryRunner.query(
      `ALTER TABLE "journey_location" ALTER COLUMN "journeyEntryAt" SET NOT NULL`
    );
    await queryRunner.query(
      `ALTER TABLE "journey_location" ALTER COLUMN "journeyEntryAt" SET DEFAULT now()`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "journey_location" ALTER COLUMN "journeyEntryAt" DROP DEFAULT`
    );
    await queryRunner.query(
      `ALTER TABLE "journey_location" ALTER COLUMN "journeyEntryAt" DROP NOT NULL`
    );
    await queryRunner.query(
      `ALTER TABLE "journey_location" ALTER COLUMN "stepEntryAt" DROP DEFAULT`
    );
    await queryRunner.query(
      `ALTER TABLE "journey_location" ALTER COLUMN "stepEntryAt" DROP NOT NULL`
    );
    await queryRunner.query(`ALTER TABLE "organization" DROP COLUMN "planId"`);
    await queryRunner.query(`DROP TABLE "organization_plan"`);
  }
}
