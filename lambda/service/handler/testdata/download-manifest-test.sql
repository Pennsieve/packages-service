-- Test data for download-manifest handler tests
-- Uses org 2, dataset 300

-- Ensure dataset exists (use id 300 to avoid conflict with cloudfront tests)
INSERT INTO "2".datasets (id, name, node_id, state, tags, contributors, status_id, created_at, updated_at) VALUES
(300, 'Download Test Dataset', 'N:dataset:dl-test', 'READY', '{}', '{}', 1, '2023-01-01 00:00:00', '2023-01-01 00:00:00')
ON CONFLICT (id) DO NOTHING;

-- Packages hierarchy:
-- root-collection (Collection, id=3000, contains children)
--   ├── child-single-file (CSV, id=3001, 1 source file)
--   └── child-multi-file  (CSV, id=3002, 2 source files)
-- standalone-file (Text, id=3003, 1 source file)
-- deleted-file (Text, id=3004, DELETED, 1 source file — should NOT appear)
-- published-file (Text, id-3005, 1 published source file, so need to use S3 versionId when creating presigned URL)

INSERT INTO "2".packages (id, name, type, state, dataset_id, parent_id, updated_at, created_at, attributes, node_id, size, owner_id, import_id) VALUES
(3000, 'root-collection', 'Collection', 'READY', 300, null, '2023-01-01 00:00:00', '2023-01-01 00:00:00', '[]', 'N:collection:dl-root', null, 1, '00000000-0000-0000-0000-000000003000'),
(3001, 'child-single-file', 'CSV', 'READY', 300, 3000, '2023-01-01 00:00:00', '2023-01-01 00:00:00', '[]', 'N:package:dl-child-single', null, 1, '00000000-0000-0000-0000-000000003001'),
(3002, 'child-multi-file', 'CSV', 'READY', 300, 3000, '2023-01-01 00:00:00', '2023-01-01 00:00:00', '[]', 'N:package:dl-child-multi', null, 1, '00000000-0000-0000-0000-000000003002'),
(3003, 'standalone-file', 'Text', 'READY', 300, null, '2023-01-01 00:00:00', '2023-01-01 00:00:00', '[]', 'N:package:dl-standalone', null, 1, '00000000-0000-0000-0000-000000003003'),
(3004, 'deleted-file', 'Text', 'DELETED', 300, null, '2023-01-01 00:00:00', '2023-01-01 00:00:00', '[]', 'N:package:dl-deleted', null, 1, '00000000-0000-0000-0000-000000003004'),
(3005, 'published-file', 'Text', 'READY', 300, null, '2023-01-01 00:00:00', '2023-01-01 00:00:00', '[]', 'N:package:dl-published', null, 1, '00000000-0000-0000-0000-000000003005')
ON CONFLICT (id) DO NOTHING;

-- Files: object_type = 'source' for downloadable files
-- child-single-file has 1 source file
INSERT INTO "2".files (id, package_id, name, file_type, s3_bucket, s3_key, object_type, size, checksum, uuid, processing_state, uploaded_state, created_at, updated_at) VALUES
(5001, 3001, 'data.csv', 'CSV', 'pennsieve-test-storage', 'org2/data.csv', 'source', 1024, '{}', '00000000-0000-0000-0000-000000005001', 'unprocessed', 'uploaded', '2023-01-01 00:00:00', '2023-01-01 00:00:00')
ON CONFLICT (id) DO NOTHING;

-- child-multi-file has 2 source files
INSERT INTO "2".files (id, package_id, name, file_type, s3_bucket, s3_key, object_type, size, checksum, uuid, processing_state, uploaded_state, created_at, updated_at) VALUES
(5002, 3002, 'part1.csv', 'CSV', 'pennsieve-test-storage', 'org2/part1.csv', 'source', 2048, '{}', '00000000-0000-0000-0000-000000005002', 'unprocessed', 'uploaded', '2023-01-01 00:00:00', '2023-01-01 00:00:00'),
(5003, 3002, 'part2.csv', 'CSV', 'pennsieve-test-storage', 'org2/part2.csv', 'source', 4096, '{}', '00000000-0000-0000-0000-000000005003', 'unprocessed', 'uploaded', '2023-01-01 00:00:00', '2023-01-01 00:00:00')
ON CONFLICT (id) DO NOTHING;

-- standalone-file has 1 source file (with .ome.tiff extension for extension test)
INSERT INTO "2".files (id, package_id, name, file_type, s3_bucket, s3_key, object_type, size, checksum, uuid, processing_state, uploaded_state, created_at, updated_at) VALUES
(5004, 3003, 'image.ome.tiff', 'OMETIFF', 'pennsieve-test-storage', 'org2/image.ome.tiff', 'source', 8192, '{}', '00000000-0000-0000-0000-000000005004', 'unprocessed', 'uploaded', '2023-01-01 00:00:00', '2023-01-01 00:00:00')
ON CONFLICT (id) DO NOTHING;

-- deleted-file has 1 source file (should not be returned because package is DELETED)
INSERT INTO "2".files (id, package_id, name, file_type, s3_bucket, s3_key, object_type, size, checksum, uuid, processing_state, uploaded_state, created_at, updated_at) VALUES
(5005, 3004, 'gone.txt', 'Text', 'pennsieve-test-storage', 'org2/gone.txt', 'source', 100, '{}', '00000000-0000-0000-0000-000000005005', 'unprocessed', 'uploaded', '2023-01-01 00:00:00', '2023-01-01 00:00:00')
ON CONFLICT (id) DO NOTHING;

-- published-file has 1 source file (with non-null versionId for publishedd file test)
INSERT INTO "2".files (id, package_id, name, file_type, s3_bucket, s3_key, published_s3_version_id, object_type, size, checksum, uuid, processing_state, uploaded_state, created_at, updated_at) VALUES
    (5006, 3005, 'published-image.ome.tiff', 'OMETIFF', 'pennsieve-test-publish', '14/files/published-image.ome.tiff', 'Pu_BlishedVersionId','source', 8192, '{}', '00000000-0000-0000-0000-000000005006', 'unprocessed', 'uploaded', '2023-01-01 00:00:00', '2023-01-01 00:00:00')
ON CONFLICT (id) DO NOTHING;