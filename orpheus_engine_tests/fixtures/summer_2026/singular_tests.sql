-- Minimal contracts for the relations referenced by Summer singular tests.
-- In particular the hourly log exposes activity_hour, not activity_date.
CREATE SCHEMA public_summer_2026_analytics;
SET search_path = public_summer_2026_analytics;
CREATE TABLE summer_unified_time_log (
    activity_hour timestamptz, program_name text, user_email text,
    raw_hours_logged numeric, logging_method text, dau numeric, dau_deduped numeric
);
CREATE TABLE daily_active_users (activity_date date, program_name text, dau bigint);
CREATE TABLE daily_active_users_deduped (activity_date date, dau bigint);
CREATE TABLE daily_hours_logged_by_program (activity_date date, program_name text, hours_logged numeric);
CREATE TABLE dashboard_daily_totals (
    activity_date date, latest_complete_date date, is_latest_complete_date boolean,
    deduped_dau bigint, total_hours_logged numeric
);
CREATE TABLE dashboard_program_daily_metrics (activity_date date, program_name text);
CREATE TABLE dashboard_data_health (source text, type text);
INSERT INTO summer_unified_time_log VALUES
    ('2026-01-02 23:00+00', 'half_life', 'missing@example.invalid', 0.125, 'journal', NULL, NULL),
    ('2026-01-03 01:00+00', 'half_life', 'missing@example.invalid', 0.125, 'journal', NULL, NULL),
    ('2026-01-02 23:00+00', 'half_life', 'marked@example.invalid', 0.25, 'journal', 1, 1),
    ('2026-01-03 01:00+00', 'half_life', 'marked@example.invalid', 0, 'daily_user_activity', 0, 0),
    ('2026-01-03 12:00+00', 'half_life', 'short@example.invalid', 0.24, 'journal', NULL, NULL),
    ('2026-01-03 12:00+00', 'other', 'other@example.invalid', 1, 'journal', 1, 1);
