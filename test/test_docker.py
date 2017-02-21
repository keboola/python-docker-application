import pytest
import os
import json
import tempfile
from keboola import docker


class TestDockerConfig:
    def test_missing_config(self):
        try:
            docker.Config('/non-existent/')
            pytest.xfail("Must raise exception.")
        except (ValueError):
            pass

    def test_missing_dir(self):
        try:
            docker.Config()
            pytest.xfail("Must raise exception.")
        except (ValueError):
            pass

    def test_get_parameters(self):
        cfg = docker.Config()
        params = cfg.get_parameters()
        assert {'fooBar': {'bar': 24, 'foo': 42}, 'baz': 'bazBar'} == params
        assert params['fooBar']['foo'] == 42
        assert params['fooBar']['bar'] == 24

    def test_get_action(self):
        cfg = docker.Config()
        action = cfg.get_action()
        assert action == 'test'

    def test_get_action_empty_config(self):
        cfg = docker.Config(os.getenv('KBC_DATADIR', '') + '/../data2/')
        action = cfg.get_action()
        assert action == ''

    def test_get_data_dir(self):
        cfg = docker.Config()
        assert os.getenv('KBC_DATADIR', '') == cfg.get_data_dir()

    def test_file_manifest(self):
        cfg = docker.Config()
        some_file = os.path.join(tempfile.mkdtemp('kbc-test') + 'someFile.txt')
        cfg.write_file_manifest(some_file, file_tags=['foo', 'bar'], is_public=True, is_permanent=False, notify=True)
        manifest_file = some_file + '.manifest'
        config = json.load(open(manifest_file))
        assert {'is_public': True, 'is_permanent': False, 'notify': True, 'tags': ['foo', 'bar']} == config
        os.remove(manifest_file)

    def test_table_manifest(self):
        cfg = docker.Config()
        some_file = os.path.join(tempfile.mkdtemp('kbc-test') + 'some-table.csv')
        cfg.write_table_manifest(some_file, 'out.c-main.some-table', primary_key=['foo', 'bar'])
        manifest_file = some_file + '.manifest'
        config = json.load(open(manifest_file))
        assert {'destination': 'out.c-main.some-table', 'primary_key': ['foo', 'bar']} == config
        os.remove(manifest_file)

    def test_input_files(self):
        cfg = docker.Config()
        files = cfg.get_input_files()
        assert len(files) == 5
        assert '21702.strip.print.gif' == files[0][-21:]

    def test_get_expected_output_files(self):
        cfg = docker.Config()
        files = cfg.get_expected_output_files()
        assert len(files) == 1
        assert 'processed.png' == files[0]['source']

    def test_get_file_manifest(self):
        cfg = docker.Config()
        files = cfg.get_input_files()
        file1 = cfg.get_file_manifest(files[0])
        assert 151971405 == file1['id']
        assert '21702.strip.print.gif' == file1['name']
        assert ['dilbert'] == file1['tags']
        file2 = cfg.get_file_manifest('151971405_21702.strip.print.gif')
        assert file1 == file2

    def test_get_input_tables(self):
        cfg = docker.Config()
        tables = cfg.get_input_tables()

        assert 2 == len(tables)
        for table in tables:
            if (table['destination'] == 'sample.csv'):
                assert 'in.c-main.test' == table['source']
                assert os.path.isfile(table['full_path'])
            else:
                assert 'in.c-main.test2' == table['source']
                assert os.path.isfile(table['full_path'])

    def test_get_table_manifest(self):
        cfg = docker.Config()
        table1 = cfg.get_table_manifest('sample.csv')
        assert 'in.c-main.test' == table1['id']
        assert 13 == len(table1['columns'])

        table2 = cfg.get_table_manifest('sample')
        assert table1 == table2

    def test_get_output_tables(self):
        cfg = docker.Config()
        tables = cfg.get_expected_output_tables()
        assert 2 == len(tables)
        assert 'results.csv' == tables[0]['source']
        assert 'results-new.csv' == tables[1]['source']

    def test_empty_storage(self):
        cfg = docker.Config(os.getenv('KBC_DATADIR', '') + '/../data2/')
        assert [] == cfg.get_expected_output_tables()
        assert [] == cfg.get_expected_output_files()
        assert [] == cfg.get_input_tables()
        assert [] == cfg.get_input_files()
        assert {} == cfg.get_parameters()

    def test_get_authorization(self):
        cfg = docker.Config()
        auth = cfg.get_authorization()
        assert auth['oauth_api']['id'] == "123456"
        assert auth['oauth_api']['credentials']["id"] == "main"

    def test_get_oauthapi_data(self):
        cfg = docker.Config()
        assert cfg.get_oauthapi_data() == {"mykey": "myval"}

    def test_get_oauthapi_appsecret(self):
        cfg = docker.Config()
        assert cfg.get_oauthapi_appsecret() == "myappsecret"

    def test_get_oauthapi_appkey(self):
        cfg = docker.Config()
        assert cfg.get_oauthapi_appkey() == "myappkey"
