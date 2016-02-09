import argparse
import json
import os
import sys

class Config(object):
    def __init__(self, dataDir=''):
        csv.register_dialect('kbc', lineterminator='\n', delimiter = ',', quotechar = '"')
        self.configData = []
        self.dataDir = ''
        if (dataDir == '' or dataDir is None):
            argparser = argparse.ArgumentParser()
            argparser.add_argument(
                '-d',
                '--data',
                dest='dataDir',
                default='',
                help='Data directory'
            )
            # unknown is to ignore extra arguments
            args, unknown = argparser.parse_known_args()
            dataDir = args.dataDir
            if (dataDir == ''):
                dataDir = os.getenv('KBC_DATA_DIR', '')
                if (dataDir == ''):
                    dataDir = '/data/'
        self.dataDir = dataDir
        try:
            self.configData = json.load(
                open(os.path.join(dataDir, 'config.json'), 'r')
            )
        except (OSError, IOError):
            raise ValueError(
                "Configuration file config.json not found, " +
                "verify that the data directory is correct." +
                "Dir: " + self.dataDir
            )

    def writeFileManifest(
            self,
            fileName,
            fileTags=[],
            isPublic=False,
            isPermanent=True,
            notify=False):
        """
        Write manifest for output file. Manifest is used for the file to be
        stored in KBC Storage. List with parsed configuration file structure is
        accessible as configData property.

        Args:
            fileName: Local file name of the file to be stored, including path.
            fileTags: List of file tags.
            isPublic: True if the file should be stored as public.
            isPermanent: False if the file should be stored only temporarily
                (for days) otherwise it will be stored until deleted.
            notify: True if members of the project should be notified
                about the file upload.
        """
        manifest = {
            'is_permanent': isPermanent,
            'is_public': isPublic,
            'tags': fileTags,
            'notify': notify
        }
        with open(fileName + '.manifest', 'w') as manifestFile:
            json.dump(manifest, manifestFile)

    def writeTableManifest(
            self,
            fileName,
            destination,
            primaryKey=[],
            indexedColumns=[]):
        """
        Write manifest for output table Manifest is used for
        the table to be stored in KBC Storage.

        Args:
            fileName: Local file name of the CSV with table data.
            destination: String name of the table in Storage.
            primaryKey: List with names of columns used for primary key.

        """
        manifest = {
            'destination': destination,
            'primary_key': primaryKey
        }
        with open(fileName + '.manifest', 'w') as manifestFile:
            json.dump(manifest, manifestFile)

    def getParameters(self):
        """
        Get arbitrary parameters passed to the application.

        Returns:
            Dict with parameters.
        """
        if ('parameters' in self.configData):
            return(self.configData['parameters'])
        else:
            return({})

    def getInputFiles(self):
        """
        Get names of input files. Returns fully classified pathnames.

        Returns:
            List with file names.
        """
        filesPath = os.path.join(self.dataDir, 'in', 'files')
        files = []
        for file in os.listdir(filesPath):
            if (os.path.isfile(os.path.join(filesPath, file)) and
                    file[-9:] != '.manifest') and file[:1] != '.':
                files.append(os.path.join(filesPath, file))
        files.sort()
        return files

    def getFileManifest(self, fileName):
        """
        Get additional file information stored in file manifest.

        Args:
            fileName: Destination file name (without .manifest extension)

        Returns:
            List with manifest options.
        """
        fileName = os.path.normpath(fileName)
        baseDir = os.path.normpath(os.path.join(self.dataDir, 'in', 'files'))
        if (fileName[0:len(baseDir)] != baseDir):
            fileName = os.path.join(baseDir, fileName)

        manifestPath = fileName + '.manifest'
        manifest = json.load(open(manifestPath))
        return(manifest)

    def getExpectedOutputFiles(self):
        """
        Get files which are supposed to be returned
        when the application finishes.

        Returns:
            List with dictionaries with output file properties.
        """
        if (('storage' in self.configData) and
                ('output' in self.configData['storage']) and
                ('files') in self.configData['storage']['output']):
            files = self.configData['storage']['output']['files']
            return(files)
        else:
            return([])

    def getInputTables(self):
        """
        Get input tables specified in the configuration file.
        Tables are identified by their destination (.csv file) or full_path.

        Returns:
            List of dictionaries with output tables properties.
        """
        if (('storage' in self.configData) and
                ('input' in self.configData['storage']) and
                ('tables') in self.configData['storage']['input']):
            tables = self.configData['storage']['input']['tables']
            for table in tables:
                table['full_path'] = os.path.normpath(
                    os.path.join(
                        self.dataDir,
                        'in',
                        'tables',
                        table['destination']
                    )
                )
            return(tables)
        else:
            return([])

    def getTableManifest(self, tableName):
        """
        Get additional table information stored in table manifest.

        Args:
            tableName: Destination table name (name of .csv file).

        Returns:
            List with manifest options.
        """
        if (tableName[-4:] != '.csv'):
            tableName += '.csv'
        manifestPath = os.path.join(
            self.dataDir,
            'in',
            'tables',
            tableName + '.manifest'
        )
        manifest = json.load(open(manifestPath))
        return(manifest)

    def getExpectedOutputTables(self):
        """
        Get tables which are supposed to be returned
        when the application finishes.

        Returns:
            List of dictionaries with expected output tables.
        """
        if (('storage' in self.configData) and
                ('output' in self.configData['storage']) and
                ('tables') in self.configData['storage']['output']):
            tables = self.configData['storage']['output']['tables']
            for table in tables:
                table['full_path'] = os.path.join(
                    self.dataDir,
                    'out',
                    'tables',
                    table['source']
                )
            return(tables)
        else:
            return([])

    def getDataDir(self):
        """
        Get current working directory.

        Returns:
            String directory name.
        """
        return(self.dataDir)