-- Migration: 003_create_trace_viewer_reader
-- Phase 9.3: Trace Viewer read-only database role
--
-- Creates a dedicated read-only PostgreSQL role for trace viewer queries.
-- This role is separate from grafana_reader to allow independent, least-privilege
-- access to the span-level views that Grafana dashboards do not need.
--
-- Apply once against the agentops database:
--   psql -h localhost -U agentops -d agentops \
--     -v trace_viewer_reader_password='<password>' \
--     -f storage-consumer/migrations/003_create_trace_viewer_reader.sql
--
-- The password supplied here must match TRACE_VIEWER_DB_PASSWORD in .env.

CREATE ROLE trace_viewer_reader WITH LOGIN PASSWORD :'trace_viewer_reader_password';

-- Allow the role to connect to the agentops database.
GRANT CONNECT ON DATABASE agentops TO trace_viewer_reader;

-- Allow the role to see objects in the analytics schema.
GRANT USAGE ON SCHEMA analytics TO trace_viewer_reader;

-- stg_telemetry_spans and int_trace_spans are dbt VIEWs that use
-- security-invoker semantics (PostgreSQL default).  The role therefore needs
-- SELECT on the view objects AND on the underlying base table so that
-- PostgreSQL can evaluate the view body on behalf of trace_viewer_reader.
GRANT SELECT ON analytics.stg_telemetry_spans TO trace_viewer_reader;
GRANT SELECT ON analytics.int_trace_spans TO trace_viewer_reader;

-- Base table grant required because stg_telemetry_spans is a security-invoker
-- view over public.telemetry_spans.
GRANT SELECT ON public.telemetry_spans TO trace_viewer_reader;

-- Default privilege grant for future and recreated analytics objects.
-- Scope is intentionally narrow: agentops role + analytics schema only.
ALTER DEFAULT PRIVILEGES
    FOR ROLE agentops
    IN SCHEMA analytics
    GRANT SELECT ON TABLES TO trace_viewer_reader;
