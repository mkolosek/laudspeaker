import { MigrationInterface, QueryRunner } from "typeorm";

export class JourneySystemSegmentCount1721445254349 implements MigrationInterface {
    name = 'JourneySystemSegmentCount1721445254349'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "workspaces" DROP CONSTRAINT "FK_a700527eb11f812d79f55907d33"`);
        await queryRunner.query(`ALTER TABLE "journey" ADD "completedSystemSegments" integer NOT NULL DEFAULT '0'`);
        await queryRunner.query(`ALTER TABLE "journey" ADD "totalSystemSegments" integer NOT NULL DEFAULT '0'`);
        await queryRunner.query(`ALTER TYPE "public"."workspace_plan_enum" RENAME TO "workspace_plan_enum_old"`);
        await queryRunner.query(`CREATE TYPE "public"."workspaces_plan_enum" AS ENUM('free', 'paid', 'enterprise')`);
        await queryRunner.query(`ALTER TABLE "workspaces" ALTER COLUMN "plan" DROP DEFAULT`);
        await queryRunner.query(`ALTER TABLE "workspaces" ALTER COLUMN "plan" TYPE "public"."workspaces_plan_enum" USING "plan"::"text"::"public"."workspaces_plan_enum"`);
        await queryRunner.query(`ALTER TABLE "workspaces" ALTER COLUMN "plan" SET DEFAULT 'free'`);
        await queryRunner.query(`DROP TYPE "public"."workspace_plan_enum_old"`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TYPE "public"."workspace_plan_enum_old" AS ENUM('free', 'paid', 'enterprise')`);
        await queryRunner.query(`ALTER TABLE "workspaces" ALTER COLUMN "plan" DROP DEFAULT`);
        await queryRunner.query(`ALTER TABLE "workspaces" ALTER COLUMN "plan" TYPE "public"."workspace_plan_enum_old" USING "plan"::"text"::"public"."workspace_plan_enum_old"`);
        await queryRunner.query(`ALTER TABLE "workspaces" ALTER COLUMN "plan" SET DEFAULT 'free'`);
        await queryRunner.query(`DROP TYPE "public"."workspaces_plan_enum"`);
        await queryRunner.query(`ALTER TYPE "public"."workspace_plan_enum_old" RENAME TO "workspace_plan_enum"`);
        await queryRunner.query(`ALTER TABLE "journey" DROP COLUMN "totalSystemSegments"`);
        await queryRunner.query(`ALTER TABLE "journey" DROP COLUMN "completedSystemSegments"`);
        await queryRunner.query(`ALTER TABLE "workspaces" ADD CONSTRAINT "FK_a700527eb11f812d79f55907d33" FOREIGN KEY ("organizationId") REFERENCES "organization"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
    }

}
