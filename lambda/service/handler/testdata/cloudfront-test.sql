-- Test data for CloudFront signed URL handler tests
-- This builds on the pre-seeded database which already contains organizations 1, 2, 3

-- Add test organizations with custom bucket configurations
-- Note: encryption_key_id is required and must match an existing key
INSERT INTO pennsieve.organizations (id, name, slug, storage_bucket, encryption_key_id) VALUES
(10, 'Test Org A', 'test-org-a', 'test-bucket-alpha', 'this-key'),
(11, 'Test Org B', 'test-org-b', 'test-bucket-beta', 'that-key')
ON CONFLICT (id) DO NOTHING;

-- Ensure organization 1 has NULL storage_bucket for default bucket testing
UPDATE pennsieve.organizations SET storage_bucket = NULL WHERE id = 1;

-- Add test datasets to existing organizations
INSERT INTO "1".datasets (id, name, node_id, state, tags, contributors, status_id, created_at, updated_at) VALUES
(100, 'Test Dataset Alpha', 'N:dataset:test-alpha', 'READY', '{}', '{}', 1, '2023-01-01 00:00:00', '2023-01-01 00:00:00'),
(101, 'Test Dataset Beta', 'N:dataset:test-beta', 'READY', '{}', '{}', 1, '2023-01-01 00:00:00', '2023-01-01 00:00:00')
ON CONFLICT (id) DO NOTHING;

INSERT INTO "2".datasets (id, name, node_id, state, tags, contributors, status_id, created_at, updated_at) VALUES
(200, 'Test Dataset Gamma', 'N:dataset:test-gamma', 'READY', '{}', '{}', 1, '2023-01-01 00:00:00', '2023-01-01 00:00:00'),
(201, 'Test Dataset Delta', 'N:dataset:test-delta', 'READY', '{}', '{}', 1, '2023-01-01 00:00:00', '2023-01-01 00:00:00')
ON CONFLICT (id) DO NOTHING;

-- Add test packages to existing organizations  
INSERT INTO "1".packages (id, name, type, state, dataset_id, parent_id, updated_at, created_at, attributes, node_id, size, owner_id, import_id) VALUES
(1000, 'test-package-alpha', 'Text', 'READY', 100, null, '2023-01-01 00:00:00', '2023-01-01 00:00:00', '[]', 'N:package:test-alpha', null, 1, '00000000-0000-0000-0000-000000001000'),
(1001, 'test-package-beta', 'Text', 'READY', 101, null, '2023-01-01 00:00:00', '2023-01-01 00:00:00', '[]', 'N:package:test-beta', null, 1, '00000000-0000-0000-0000-000000001001')
ON CONFLICT (id) DO NOTHING;

INSERT INTO "2".packages (id, name, type, state, dataset_id, parent_id, updated_at, created_at, attributes, node_id, size, owner_id, import_id) VALUES
(2000, 'test-package-gamma', 'Text', 'READY', 200, null, '2023-01-01 00:00:00', '2023-01-01 00:00:00', '[]', 'N:package:test-gamma', null, 1, '00000000-0000-0000-0000-000000002000'),
(2001, 'test-package-delta', 'Text', 'READY', 201, null, '2023-01-01 00:00:00', '2023-01-01 00:00:00', '[]', 'N:package:test-delta', null, 1, '00000000-0000-0000-0000-000000002001')
ON CONFLICT (id) DO NOTHING;