-- Base setup file that ensures required datasets exist for all tests
-- This file should be run before any test-specific SQL files

-- Ensure organization schemas exist (they should from seed, but be safe)
CREATE SCHEMA IF NOT EXISTS "1";
CREATE SCHEMA IF NOT EXISTS "2";

-- Create required dataset_status entries first
INSERT INTO "1".dataset_status (id, name, display_name, color, created_at, updated_at)
VALUES (1, 'NO_STATUS', 'No Status', '#71747C', NOW(), NOW())
ON CONFLICT (id) DO NOTHING;

INSERT INTO "2".dataset_status (id, name, display_name, color, created_at, updated_at)
VALUES (1, 'NO_STATUS', 'No Status', '#71747C', NOW(), NOW())
ON CONFLICT (id) DO NOTHING;

-- Create base datasets that packages depend on
-- Using ON CONFLICT to make this idempotent
INSERT INTO "1".datasets (id, name, state, node_id, created_at, updated_at, status_id, size, etag, description, license, tags, contributors, banner_id, readme_id) 
VALUES (
    1, 
    'Test Dataset 1',
    'READY',
    'N:dataset:test-1',
    NOW(),
    NOW(),
    1,
    0,
    NOW(),
    'Test dataset for packages',
    '',
    ARRAY[]::varchar[],
    ARRAY[]::varchar[],
    NULL,
    NULL
) ON CONFLICT (id) DO NOTHING;

INSERT INTO "2".datasets (id, name, state, node_id, created_at, updated_at, status_id, size, etag, description, license, tags, contributors, banner_id, readme_id)
VALUES (
    1,
    'Test Dataset 1', 
    'READY',
    'N:dataset:test-1',
    NOW(),
    NOW(),
    1,
    0,
    NOW(),
    'Test dataset for packages',
    '',
    ARRAY[]::varchar[],
    ARRAY[]::varchar[],
    NULL,
    NULL
) ON CONFLICT (id) DO NOTHING;

-- Add any other commonly needed test data here