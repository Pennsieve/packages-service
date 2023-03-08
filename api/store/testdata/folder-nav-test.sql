INSERT INTO "2".packages (id, name, type, state, dataset_id, parent_id, updated_at, created_at, attributes, node_id, size, owner_id, import_id) VALUES
-- Level Zero
(1, 'root-file.txt-1', 'Text', 'UPLOADED', 1, null, '2023-02-02 04:31:05.839616', '2023-01-20 19:23:03.309580', '[{"key": "subtype", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:ae253796-256a-4b9e-ba80-1c4c5a2afe6b', null, 1, '4d369199-d98a-4b44-b9b4-6c096c78e358'),
(2, 'root-file-deleted-1.txt', 'Text', 'DELETED', 1, null, '2023-02-02 04:31:05.839611', '2023-01-20 19:23:04.369929', '[{"key": "subtype", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:5ff98fab-d0d6-4cac-9f11-4b6ff50788e8', null, 1, 'b16b329f-52aa-471c-aad4-c679a4fdf5b2'),
(3, 'root-dir-empty-1', 'Collection', 'READY', 1, null, '2023-02-02 19:45:32.337296', '2023-02-02 19:44:02.436354', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:36cb9fb0-f72a-42fd-bcac-959ecb866279', null, 1, '3f580989-cb11-4a8c-b44f-161ff7b7684b'),
(4, 'root-dir-deleted-1', 'Collection', 'DELETED', 1, null, '2023-02-02 19:45:07.113070', '2023-02-02 19:44:02.692212', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:82c127ca-b72b-4d8b-a0c3-a9e4c7b14654', null, 1, '519d0241-e544-4c89-9524-1ea94a792d28'),
(5, 'root-dir-1', 'Collection', 'READY', 1, null, '2023-02-02 19:45:32.297609', '2023-02-02 19:44:01.835701', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:180d4f48-ea2b-435c-ac69-780eeaf89745', null, 1, '2b02f210-2d77-49f6-acb8-efd5937e1286'),
(6, 'root-dir-2', 'Collection', 'READY', 1, null, '2023-02-02 19:45:32.297609', '2023-02-02 19:44:02.128291', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:0f197fab-cb7b-4414-8f7c-27d7aafe7c53', null, 1, '6c35438d-9f7e-4883-a7c7-f16369e895b3'),
-- Level One
--   root-dir-1 (id 5)
(7, 'one-file-1.csv', 'CSV', 'UPLOADED', 1, 5, '2023-02-02 19:45:07.336741', '2023-02-02 19:44:03.621534', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:b58fbcd5-0db2-4c7f-8ea5-d66d7c1d542f', null, 1, 'b77d32c5-18ef-4d63-b6df-07758655d79e'),
(8, 'one-file-deleted-1.csv', 'CSV', 'DELETED', 1, 5, '2023-02-02 19:45:07.113352', '2023-02-02 19:44:03.641141', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:7a1e270b-eb23-4b26-b106-d32101399a8a', null, 1, '3886a002-976b-4fc1-8307-d494f4bf795e'),
(9, 'one-dir-1', 'Collection', 'READY', 1, 5, '2023-02-02 19:45:06.932059', '2023-02-02 19:44:02.142319', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:e9bfe050-b375-43a1-91ec-b519439ad011', null, 1, 'ec7d2abe-fced-4521-b4d9-79901d99aab1'),
(10, 'one-dir-deleted-1', 'Collection', 'DELETED', 1, 5, '2023-02-02 19:45:07.470546', '2023-02-02 19:44:01.946510', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:b8ab062e-e7d0-4668-b098-c322ae460820', null, 1, '96dfe4b9-0f2e-4a6c-a816-7c3b20fe8460'),
--   root-dir-2 (id 6)
(11, 'one-file-1', 'CSV', 'UPLOADED', 1, 6, '2023-02-02 19:45:07.399784', '2023-02-02 19:44:03.136698', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:55ca012d-43c5-4997-b68a-a87edad67cb9', null, 1, 'd9231979-61ce-4410-b59a-0e871bf57afc'),
(12, 'one-dir-1', 'Collection', 'READY', 1, 6, '2023-02-02 19:45:07.471454', '2023-02-02 19:44:03.547259', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:3e59dad2-f061-4aff-aa8e-f986908f51b3', null, 1, '000cad2a-cdf7-44d3-a1d7-2c589820ea1c'),
--   root-dir-deleted-1 (id 4)
(13, 'one-file-deleted-1.csv', 'CSV', 'DELETED', 1, 4, '2023-02-02 19:45:07.163698', '2023-02-02 19:44:02.549572', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:8d18065b-e7d7-4792-8de4-6fc7ecb79a46', null, 1, 'd9506458-716a-4460-8ea5-0d3b256fcc93'),
(14, 'one-file-deleted-2', 'Unsupported', 'DELETED', 1, 4, '2023-02-02 19:45:07.159605', '2023-02-02 19:44:00.996318', '[{"key": "subtype", "fixed": false, "value": "MS Excel", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Excel", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:40443908-a2e1-474c-8367-d04ffbda7947', null, 1, 'de3c748d-a812-4f66-8d06-31181ea4df22'),
(15, 'one-dir-deleted-1', 'Collection', 'DELETED', 1, 4, '2023-02-02 19:45:07.384286', '2023-02-02 19:44:02.096857', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:8397346c-b824-4ee7-a49d-892860892d41', null, 1, '1a1df8b0-88ca-4e76-9e22-fbfa640740c2'),
-- Level Two
--  root-dir-1/one-dir-deleted-1 (id 10)
(16, 'two-file-deleted-1.csv', 'CSV', 'DELETED', 1, 10, '2023-02-02 19:45:07.163184', '2023-02-02 19:44:03.199927', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:bb5970ae-594d-42d2-a223-f38a55eaa3b8', null, 1, '9ecde465-c09f-48c7-bb4d-448e7bb33d9f'),
--  root-dir-1/one-dir-1 (id 9)
(17, 'two-file-1.csv', 'CSV', 'UPLOADED', 1, 9, '2023-02-02 19:45:07.226817', '2023-02-02 19:44:02.001045', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:3e3e3746-ac22-4a98-9a92-0a7b7144a902', null, 1, '8b9522bd-d94c-4f5e-9286-5f7dcb6836b5'),
(18, 'two-file-2.csv', 'CSV', 'UPLOADED', 1, 9, '2023-02-02 19:45:07.438179', '2023-02-02 19:44:03.247496', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:e32ef0bf-37ef-4c54-99d6-44379809212e', null, 1, '95e5c82c-3a6b-4635-968f-f3e454225e72'),
(19, 'two-file-deleted-1.csv', 'CSV', 'DELETED', 1, 9, '2023-02-02 19:45:07.226619', '2023-02-02 19:44:02.495479', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:b234f34b-a827-4df1-ac79-e9c0db53915c', null, 1, 'f69163b8-44da-466a-ab03-137c5507a9eb'),
(20, 'two-file-deleted-2.csv', 'CSV', 'DELETED', 1, 9, '2023-02-02 19:45:07.471212', '2023-02-02 19:44:02.946175', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:06d2e3d0-e084-4866-8bfc-206655ec4d5c', null, 1, 'a37eb1ac-da6a-4726-926b-76411e91ef41'),
(21, 'two-dir-1', 'Collection', 'READY', 1, 9, '2023-02-02 19:45:07.164210', '2023-02-02 19:44:03.282676', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:113d3c44-af35-408f-9fcc-0e4aa0b20a5d', null, 1, 'fe7acb21-4478-4019-9dac-a6666734bca1'),
(22, 'two-dir-deleted-1', 'Collection', 'DELETED', 1, 9, '2023-02-02 19:45:07.531317', '2023-02-02 19:44:01.929567', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:a3d2d4a4-039c-4525-b99f-148690006b4f', null, 1, '02250dd8-6a5a-44f4-b646-324de6740ad6'),
--   root-dir-2/one-dir-1 (id 12)
(23, 'two-file-1.csv', 'CSV', 'UPLOADED', 1, 12, '2023-02-02 19:45:07.386185', '2023-02-02 19:44:03.163983', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:909d86f6-6cb6-4f88-b92e-129ef6748125', null, 1, '40bea8f2-6510-41cd-9d08-5f955e5c581a'),
(24, 'two-dir-1', 'Collection', 'READY', 1, 12, '2023-02-02 19:45:06.997361', '2023-02-02 19:44:03.614545', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:cf6d1989-8dd3-417a-946f-ef7c692085df', null, 1, 'fe2748ad-216a-4d59-91d2-9a95a1cfbee8'),
--   root-dir-deleted-1/one-dir-deleted-1 (id 15)
(25, 'two-file-deleted-1.csv', 'CSV', 'DELETED', 1, 15, '2023-02-02 19:45:07.531309', '2023-02-02 19:44:02.653122', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:d9ee5d8f-0f27-4179-ae9e-8b914a719543', null, 1, '5a795d26-58ce-4c82-9f56-7158b8ae2175'),
(26, 'two-dir-deleted-1', 'Collection', 'DELETED', 1, 15, '2023-02-02 19:45:07.531851', '2023-02-02 19:44:03.194710', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:92907aeb-a524-4b74-960c-ddda270bf1ce', null, 1, '17e00ce2-5960-4183-8b44-86e0de4dd35a'),
-- Level Three
--   root-dir-1/one-dir-1/two-dir-deleted-1 (id 22)
(27, 'three-file-deleted-1.csv', 'CSV', 'DELETED', 1, 22, '2023-02-02 19:45:07.293872', '2023-02-02 19:44:02.499264', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:14298d95-0b87-4b15-b8fe-3007980657df', null, 1, 'de308259-09ec-47f9-aa29-da7ef1e7513c'),
--   root-dir-1/one-dir-1/two-dir-1 (id 21)
(28, 'three-file-1.csv', 'CSV', 'UPLOADED', 1, 21, '2023-02-02 19:45:07.436161', '2023-02-02 19:44:02.434730', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:766087ef-a88a-492e-b8ee-b6b9e4ce0178', null, 1, '9a180539-6263-460e-ac93-0a0ecc2220c8'),
(29, 'three-file-2.txt', 'Text', 'UPLOADED', 1, 21, '2023-02-02 23:02:07.693571', '2023-02-02 23:01:33.374589', '[{"key": "subtype", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:67e19ed8-581e-4bd1-ae00-908a704a27e8', null, 1, '010a903b-8bd1-4ef0-81c9-140485b09e9a'),
(30, 'three-file-deleted-1.txt', 'Text', 'DELETED', 1, 21, '2023-02-02 23:02:07.594921', '2023-02-02 23:01:33.865402', '[{"key": "subtype", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:53c00fad-426e-42d4-b242-f5237d2eec64', null, 1, 'a9034486-f7d4-468b-95d2-c6b68502e36d'),
(31, 'three-dir-1', 'Collection', 'READY', 1, 21, '2023-02-02 23:02:07.595175', '2023-02-02 23:01:33.818122', '[{"key": "subtype", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:98d2c5e1-0be5-48e1-bbc0-10290e8fc6a0', null, 1, 'c9eb46ef-41bf-452a-b466-3345cbe016e4'),
(32, 'three-dir-2', 'Collection', 'READY', 1, 21, '2023-02-02 23:02:07.631746', '2023-02-02 23:01:33.981385', '[{"key": "subtype", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:43e7f46a-5d80-4a95-9abb-af1028c0cef6', null, 1, 'f7f40326-2b03-443f-9bdf-f9c6582ffd61'),
(33, 'three-dir-deleted-1', 'Collection', 'DELETED', 1, 21, '2023-02-02 23:02:07.626266', '2023-02-02 23:01:33.865838', '[{"key": "subtype", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:ab0ae7fd-96d1-4f61-af0c-f7b6e7ea7639', null, 1, 'c641c677-6c48-4dc1-bec4-5032f849d680'),
(34, 'three-dir-deleted-2', 'Collection', 'DELETED', 1, 21, '2023-02-02 23:02:07.596783', '2023-02-02 23:01:34.000072', '[{"key": "subtype", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Text", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:f4136743-e930-401e-88bb-e7ef34789a88', null, 1, '8fac31b8-e6d3-4fd5-b11c-10bd65967aad'),
--   root-dir-2/one-dir-1/two-dir-1 (id 24)
(35, 'three-dir-1', 'Collection', 'READY', 1, 24, '2023-02-03 03:23:16.382752', '2023-02-03 03:22:15.575859', '[{"key": "subtype", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:collection:d6542ca3-31a4-473f-a7ab-490ca4fddc63', null, 1, 'cbf42bc8-e6c3-41a2-a403-6fbb9ab1b6b4'),
--   root-dir-deleted-1/one-dir-deleted-1/two-dir-deleted-1 (id 26)
(36, 'three-file-deleted-1.png', 'Image', 'DELETED', 1, 26, '2023-02-03 03:23:16.377736', '2023-02-03 03:22:17.424529', '[{"key": "subtype", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:6974bfb6-2714-4f80-8942-c34357dfeee0', null, 1, '018630aa-3526-4849-8482-1d78dfd30d75'),
-- Level Four
--   root-dir-1/one-dir-1/two-dir-1/three-dir-deleted-2 (id 34)
(37, 'four-file-deleted-1.png', 'Image', 'DELETED', 1, 34, '2023-02-03 03:23:45.866362', '2023-02-03 03:22:17.673475', '[{"key": "subtype", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:c4d0049b-4cf8-4729-935c-67e9701d33b8', null, 1, '4ee73feb-09e6-48eb-ae19-35ed107e16e1'),
--   root-dir-1/one-dir-1/two-dir-1/three-dir-deleted-1 (id 33)
(38, 'four-file-deleted-1', 'Unsupported', 'DELETED', 1, 33, '2023-02-06 15:49:44.594946', '2023-02-06 15:49:16.643147', '[{"key": "subtype", "fixed": false, "value": "MS Excel", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Excel", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:67c7567e-183e-4701-8543-8630aba5fbc2', null, 1, 'bf38f5dc-4407-404d-a7ec-bac305957bf6'),
--   root-dir-1/one-dir-1/two-dir-1/three-dir-2 (id 32)
(39, 'four-file-1.csv', 'CSV', 'UPLOADED', 1, 32, '2023-02-06 15:50:13.517485', '2023-02-06 15:49:17.389951', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:56501b51-1708-456d-996f-4480466dcb69', null, 1, 'de7ff7cb-7a86-47e5-bcde-2b7ac87cbccd'),
(40, 'four-file-2.csv', 'CSV', 'UPLOADED', 1, 32, '2023-02-06 15:50:13.526042', '2023-02-06 15:49:17.634604', '[{"key": "subtype", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Tabular", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:a46d1006-7498-41bb-9385-8a0f892f3666', null, 1, 'ceb8a635-3ae8-4105-b8ce-596956a2f4e9'),
--   root-dir-1/one-dir-1/two-dir-1/three-dir-1 (id 31)
(41, 'four-file-1.png', 'Image', 'UPLOADED', 1, 31, '2023-02-13 18:48:32.047377', '2023-02-13 18:47:50.752926', '[{"key": "subtype", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:b8578c6b-929d-4b1a-876b-5e0a87cfa3ad', null, 1, 'e5602b5f-9f81-4be3-a06f-102c989b1274'),
(42, 'four-file-deleted-1.png', 'Image', 'DELETED', 1, 31, '2023-02-13 18:48:32.049565', '2023-02-13 18:47:51.677481', '[{"key": "subtype", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:8180a4dd-bf19-4476-ae54-79018dc14821', null, 1, '43508de3-29e3-43ea-8913-4afaf1ac826e'),
--   root-dir-2/one-dir-1/two-dir-1/three-dir-1 (id 35)
(43, 'four-file-1.png', 'Image', 'UPLOADED', 1, 35, '2023-02-13 18:48:48.708248', '2023-02-13 18:47:51.874504', '[{"key": "subtype", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:8cd9f6ba-9639-456d-997f-0eb712936293', null, 1, 'd42e473e-d0eb-4a3c-ba34-f79acc8ae44a'),
(44, 'four-file-2.png', 'Image', 'UPLOADED', 1, 35, '2023-02-13 18:48:48.719635', '2023-02-13 18:47:51.647923', '[{"key": "subtype", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}, {"key": "icon", "fixed": false, "value": "Image", "hidden": true, "category": "Pennsieve", "dataType": "string"}]', 'N:package:416e9542-6aa5-4bdc-bf08-7767e30cc30e', null, 1, '6e363faa-68b9-4721-b48a-0992b8456d1a');
