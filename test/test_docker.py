import pytest
import sys
import os
import json
import tempfile
from keboola import docker

class TestDockerConfig:
    def test_missingConfig(self, dataDir):
        try:
            cfg = docker.Config('/non-existent/')
            pytest.xfail("Must raise exception.")
        except (ValueError):
            pass


    def test_missingDir(self, dataDir):
        try:
            cfg = docker.Config()
            pytest.xfail("Must raise exception.")
        except (ValueError):
            pass


    def test_getParameters(self, dataDir):
        cfg = docker.Config(dataDir)
        params = cfg.getParameters()
        assert {'fooBar': {'bar': 24, 'foo': 42}, 'baz': 'bazBar'} == params
        assert params['fooBar']['foo'] == 42
        assert params['fooBar']['bar'] == 24
   
   
    def test_fileManifest(self, dataDir):
        cfg = docker.Config(dataDir)
        someFile = os.path.join(tempfile.mkdtemp('kbc-test') + 'someFile.txt')
        cfg.writeFileManifest(someFile, fileTags = ['foo', 'bar'], isPublic = True, isPermanent = False, notify = True)
        manifestFile = someFile + '.manifest'
        config = json.load(open(manifestFile))
        assert {'is_public': True, 'is_permanent': False, 'notify': True, 'tags': ['foo', 'bar']} == config
        os.remove(manifestFile)
        
   
    def test_tableManifest(self, dataDir):
        cfg = docker.Config(dataDir)
        someFile = os.path.join(tempfile.mkdtemp('kbc-test') + 'some-table.csv')
        cfg.writeTableManifest(someFile, 'out.c-main.some-table', primaryKey = ['foo', 'bar'])
        manifestFile = someFile + '.manifest'
        config = json.load(open(manifestFile))
        assert {'destination': 'out.c-main.some-table', 'primary_key': ['foo', 'bar']} == config
        os.remove(manifestFile)


    def test_inputFiles(self, dataDir):
        cfg = docker.Config(dataDir)
        files = cfg.getInputFiles()
        assert len(files) == 5
        assert '21702.strip.print.gif' == files[0][-21:] 
    

    def test_getExpectedOutputFiles(self, dataDir):
        cfg = docker.Config(dataDir)
        files = cfg.getExpectedOutputFiles()
        assert len(files) == 1
        assert 'processed.png' == files[0]['source']    
    
    
    def test_getFileManifest(self, dataDir):
        cfg = docker.Config(dataDir)
        files = cfg.getInputFiles()
        file1 = cfg.getFileManifest(files[0])
        assert 151971405 == file1['id']
        assert '21702.strip.print.gif' == file1['name']
        assert ['dilbert'] == file1['tags']
        file2 = cfg.getFileManifest('151971405_21702.strip.print.gif')
        assert file1 == file2    


    def test_getInputTables(self, dataDir):    
        cfg = docker.Config(dataDir)
        tables = cfg.getInputTables()
        
        assert 2 == len(tables)
        for table in tables:
            if (table['destination'] == 'sample.csv'):
                assert 'in.c-main.test' == table['source']
                assert True == os.path.isfile(table['full_path'])
            else:
                assert 'in.c-main.test2' == table['source']
                assert True == os.path.isfile(table['full_path'])       
    
    
    def test_getTableManifest(self, dataDir):
        cfg = docker.Config(dataDir)
        table1 = cfg.getTableManifest('sample.csv')
        assert 'in.c-main.test' == table1['id']
        assert 13 == len(table1['columns'])
    
        table2 = cfg.getTableManifest('sample')
        assert table1 == table2
    
    
    def test_getOutputTables(self, dataDir):
        cfg = docker.Config(dataDir)
        tables = cfg.getExpectedOutputTables()
        assert 2 == len(tables)
        assert 'results.csv' == tables[0]['source']
        assert 'results-new.csv' == tables[1]['source']
