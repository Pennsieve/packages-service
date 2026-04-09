-- Test data for viewer assets handler tests

-- Dataset for assets tests (org 2)
INSERT INTO "2".datasets (id, name, state, node_id, created_at, updated_at, status_id, size, etag, description, license, tags, contributors, banner_id, readme_id)
VALUES (
    200,
    'Assets Test Dataset',
    'READY',
    'N:dataset:assets-test',
    NOW(), NOW(), 1, 0, NOW(), 'Dataset for viewer assets tests', '', ARRAY[]::varchar[], ARRAY[]::varchar[], NULL, NULL
) ON CONFLICT (id) DO NOTHING;

-- Packages to link assets to
INSERT INTO "2".packages (id, name, type, state, dataset_id, node_id, owner_id, created_at, updated_at)
VALUES
    (5001, 'Package A', 'Image', 'READY', 200, 'N:package:pkg-a', 1, NOW(), NOW()),
    (5002, 'Package B', 'Image', 'READY', 200, 'N:package:pkg-b', 1, NOW(), NOW()),
    (5003, 'Package C', 'Image', 'READY', 200, 'N:package:pkg-c', 1, NOW(), NOW())
ON CONFLICT (id) DO NOTHING;

-- Organization entry for storage bucket resolution
INSERT INTO pennsieve.organizations (id, name, slug, node_id, created_at, updated_at)
VALUES (2, 'Test Org', 'test-org', 'N:organization:test-org', NOW(), NOW())
ON CONFLICT (id) DO NOTHING;