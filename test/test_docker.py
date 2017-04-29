import unittest
import os
import json
import tempfile
import csv
from Keboola import docker

class TestDockerConfig(unittest.TestCase):
    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data1')
        os.environ["KBC_DATADIR"] = path

    def test_missing_config(self):
        with self.assertRaisesRegex(ValueError, "Configuration file config.json not found"):
            docker.Config('/non-existent/')

    def test_missing_dir(self):
        os.environ["KBC_DATADIR"] = ""
        with self.assertRaisesRegex(ValueError, "Configuration file config.json not found"):
            docker.Config()

    def test_get_parameters(self):
        cfg = docker.Config()
        params = cfg.get_parameters()
        self.assertEqual({'fooBar': {'bar': 24, 'foo': 42}, 'baz': 'bazBar'}, params)
        self.assertEqual(params['fooBar']['foo'], 42)
        self.assertEqual(params['fooBar']['bar'], 24)

    def test_get_action(self):
        cfg = docker.Config()
        action = cfg.get_action()
        self.assertEqual(action, 'test')

    def test_get_action_empty_config(self):
        cfg = docker.Config(os.path.join(os.getenv('KBC_DATADIR', ''), '..', 'data2'))
        action = cfg.get_action()
        self.assertEqual(action, '')

    def test_get_data_dir(self):
        cfg = docker.Config()
        self.assertEqual(os.getenv('KBC_DATADIR', ''), cfg.get_data_dir())

    def test_file_manifest(self):
        cfg = docker.Config()
        some_file = os.path.join(tempfile.mkdtemp('kbc-test') + 'someFile.txt')
        cfg.write_file_manifest(some_file, file_tags=['foo', 'bar'], is_public=True, is_permanent=False, notify=True)
        manifest_filename = some_file + '.manifest'
        with open(manifest_filename) as manifest_file:
            config = json.load(manifest_file)
        self.assertEqual(
            {'is_public': True, 'is_permanent': False, 'notify': True, 'tags': ['foo', 'bar']},
            config
        )
        os.remove(manifest_filename)

    def test_table_manifest(self):
        cfg = docker.Config()
        some_file = os.path.join(tempfile.mkdtemp('kbc-test') + 'some-table.csv')
        cfg.write_table_manifest(some_file, 'out.c-main.some-table', primary_key=['foo', 'bar'])
        manifest_filename = some_file + '.manifest'
        with open(manifest_filename) as manifest_file:
            config = json.load(manifest_file)
        self.assertEqual(
            {'destination': 'out.c-main.some-table', 'primary_key': ['foo', 'bar']},
            config
        )
        os.remove(manifest_filename)

    def test_input_files(self):
        cfg = docker.Config()
        files = cfg.get_input_files()
        self.assertEqual(len(files), 5)
        self.assertEqual('21702.strip.print.gif', files[0][-21:])

    def test_get_expected_output_files(self):
        cfg = docker.Config()
        files = cfg.get_expected_output_files()
        self.assertEqual(len(files), 1)
        self.assertEqual('processed.png', files[0]['source'])

    def test_get_file_manifest(self):
        cfg = docker.Config()
        files = cfg.get_input_files()
        file1 = cfg.get_file_manifest(files[0])
        self.assertEqual(151971405, file1['id'])
        self.assertEqual('21702.strip.print.gif', file1['name'])
        self.assertEqual(['dilbert'], file1['tags'])
        file2 = cfg.get_file_manifest('151971405_21702.strip.print.gif')
        self.assertEqual(file1, file2)

    def test_get_input_tables(self):
        cfg = docker.Config()
        tables = cfg.get_input_tables()

        self.assertEqual(len(tables), 2)
        for table in tables:
            if table['destination'] == 'sample.csv':
                self.assertEqual(table['source'], 'in.c-main.test')
                self.assertTrue(os.path.isfile(table['full_path']))
            else:
                self.assertEqual('in.c-main.test2', table['source'])
                self.assertTrue(os.path.isfile(table['full_path']))

    def test_get_table_manifest(self):
        cfg = docker.Config()
        table1 = cfg.get_table_manifest('sample.csv')
        self.assertEqual('in.c-main.test', table1['id'])
        self.assertEqual(len(table1['columns']), 13)

        table2 = cfg.get_table_manifest('sample')
        self.assertEqual(table1, table2)

    def test_get_output_tables(self):
        cfg = docker.Config()
        tables = cfg.get_expected_output_tables()
        self.assertEqual(len(tables), 2)
        self.assertEqual(tables[0]['source'], 'results.csv')
        self.assertEqual(tables[1]['source'], 'results-new.csv')

    def test_empty_storage(self):
        cfg = docker.Config(os.path.join(os.getenv('KBC_DATADIR', ''), '..', 'data2'))
        self.assertEqual(cfg.get_expected_output_tables(), [])
        self.assertEqual(cfg.get_expected_output_files(), [])
        self.assertEqual(cfg.get_input_tables(), [])
        self.assertEqual(cfg.get_input_files(), [])
        self.assertEqual(cfg.get_parameters(), {})

    def test_get_authorization(self):
        cfg = docker.Config()
        auth = cfg.get_authorization()
        self.assertEqual(auth['oauth_api']['id'], "123456")
        self.assertEqual(auth['oauth_api']['credentials']["id"], "main")

    def test_get_oauthapi_data(self):
        cfg = docker.Config()
        self.assertEqual(cfg.get_oauthapi_data(), {"mykey": "myval"})

    def test_get_oauthapi_appsecret(self):
        cfg = docker.Config()
        self.assertEqual(cfg.get_oauthapi_appsecret(), "myappsecret")

    def test_get_oauthapi_appkey(self):
        cfg = docker.Config()
        self.assertEqual(cfg.get_oauthapi_appkey(), "myappkey")

    def test_register_csv_dialect(self):
        docker.Config().register_csv_dialect()
        self.assertIn("kbc", csv.list_dialects())

if __name__ == '__main__':
    unittest.main()