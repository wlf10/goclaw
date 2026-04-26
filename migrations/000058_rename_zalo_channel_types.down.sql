-- Reverse of 000057 up: zalo_oa → zalo_oauth; zalo_bot → zalo_oa.
-- Uses the same sentinel-swap pattern.
--
-- Idempotency guard: only swap when 'zalo_bot' rows still exist (post-up
-- state). Without the guard, running `migrate down` after fresh inserts
-- with the new 'zalo_oa' name would silently flip live OA rows back to
-- the legacy 'zalo_oauth' name. Mirrors up.sql's EXISTS guard.

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM channel_instances WHERE channel_type = 'zalo_bot') THEN
    UPDATE channel_instances SET channel_type = 'zalo_oa_tmp' WHERE channel_type = 'zalo_oa';
    UPDATE channel_instances SET channel_type = 'zalo_oa'     WHERE channel_type = 'zalo_bot';
    UPDATE channel_instances SET channel_type = 'zalo_oauth'  WHERE channel_type = 'zalo_oa_tmp';
  END IF;
END $$;
