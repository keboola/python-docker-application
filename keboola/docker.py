import argparse
import json
import os
import csv


class Config(object):
    def __init__(self, data_dir=''):
        csv.register_dialect('kbc', lineterminator='\n', delimiter=',', quotechar='"')
        self.config_data = []
        self.data_dir = ''
        if (data_dir == '' or data_dir is None):
            argparser = argparse.ArgumentParser()
            argparser.add_argument(
                '-d',
                '--data',
                dest='data_dir',
                default='',
                help='Data directory'
            )
            # unknown is to ignore extra arguments
            args, unknown = argparser.parse_known_args()
            data_dir = args.data_dir
            if (data_dir == ''):
                data_dir = os.getenv('KBC_DATADIR', '')
                if (data_dir == ''):
                    data_dir = os.getenv('KBC_DATA_DIR', '')
                    if (data_dir == ''):
                        data_dir = '/data/'
        self.data_dir = data_dir
        try:
            self.config_data = json.load(
                open(os.path.join(data_dir, 'config.json'), 'r')
            )
        except (OSError, IOError):
            raise ValueError(
                "Configuration file config.json not found, " +
                "verify that the data directory is correct." +
                "Dir: " + self.data_dir
            )

    def write_file_manifest(
            self,
            file_name,
            file_tags=[],
            is_public=False,
            is_permanent=True,
            notify=False):
        """
        Write manifest for output file. Manifest is used for the file to be
        stored in KBC Storage. List with parsed configuration file structure is
        accessible as config_data property.

        Args:
            file_name: Local file name of the file to be stored, including path.
            file_tags: List of file tags.
            is_public: True if the file should be stored as public.
            is_permanent: False if the file should be stored only temporarily
                (for days) otherwise it will be stored until deleted.
            notify: True if members of the project should be notified
                about the file upload.
        """
        manifest = {
            'is_permanent': is_permanent,
            'is_public': is_public,
            'tags': file_tags,
            'notify': notify
        }
        with open(file_name + '.manifest', 'w') as manifest_file:
            json.dump(manifest, manifest_file)

    def write_table_manifest(
            self,
            file_name,
            destination,
            primary_key=[]):
        """
        Write manifest for output table Manifest is used for
        the table to be stored in KBC Storage.

        Args:
            file_name: Local file name of the CSV with table data.
            destination: String name of the table in Storage.
            primary_key: List with names of columns used for primary key.
        """
        manifest = {
            'destination': destination,
            'primary_key': primary_key
        }
        with open(file_name + '.manifest', 'w') as manifest_file:
            json.dump(manifest, manifest_file)

    def get_parameters(self):
        """
        Get arbitrary parameters passed to the application.

        Returns:
            Dict with parameters.
        """
        if ('parameters' in self.config_data):
            return(self.config_data['parameters'])
        else:
            return({})

    def get_action(self):
        """
        Get action parameter passed to the configuration.

        Returns:
            String.
        """
        if ('action' in self.config_data):
            return(self.config_data['action'])
        else:
            return('')

    def get_input_files(self):
        """
        Get names of input files. Returns fully classified pathnames.

        Returns:
            List with file names.
        """
        files_path = os.path.join(self.data_dir, 'in', 'files')
        files = []
        for file in os.listdir(files_path):
            if (os.path.isfile(os.path.join(files_path, file)) and
                    file[-9:] != '.manifest') and file[:1] != '.':
                files.append(os.path.join(files_path, file))
        files.sort()
        return files

    def get_file_manifest(self, file_name):
        """
        Get additional file information stored in file manifest.

        Args:
            file_name: Destination file name (without .manifest extension)

        Returns:
            List with manifest options.
        """
        file_name = os.path.normpath(file_name)
        base_dir = os.path.normpath(os.path.join(self.data_dir, 'in', 'files'))
        if (file_name[0:len(base_dir)] != base_dir):
            file_name = os.path.join(base_dir, file_name)

        manifest_path = file_name + '.manifest'
        manifest = json.load(open(manifest_path))
        return(manifest)

    def get_expected_output_files(self):
        """
        Get files which are supposed to be returned
        when the application finishes.

        Returns:
            List with dictionaries with output file properties.
        """
        if (('storage' in self.config_data) and
                ('output' in self.config_data['storage']) and
                ('files') in self.config_data['storage']['output']):
            files = self.config_data['storage']['output']['files']
            return(files)
        else:
            return([])

    def get_input_tables(self):
        """
        Get input tables specified in the configuration file.
        Tables are identified by their destination (.csv file) or full_path.

        Returns:
            List of dictionaries with output tables properties.
        """
        if (('storage' in self.config_data) and
                ('input' in self.config_data['storage']) and
                ('tables') in self.config_data['storage']['input']):
            tables = self.config_data['storage']['input']['tables']
            for table in tables:
                table['full_path'] = os.path.normpath(
                    os.path.join(
                        self.data_dir,
                        'in',
                        'tables',
                        table['destination']
                    )
                )
            return(tables)
        else:
            return([])

    def get_table_manifest(self, table_name):
        """
        Get additional table information stored in table manifest.

        Args:
            table_name: Destination table name (name of .csv file).

        Returns:
            List with manifest options.
        """
        if (table_name[-4:] != '.csv'):
            table_name += '.csv'
        manifest_path = os.path.join(
            self.data_dir,
            'in',
            'tables',
            table_name + '.manifest'
        )
        manifest = json.load(open(manifest_path))
        return(manifest)

    def get_expected_output_tables(self):
        """
        Get tables which are supposed to be returned
        when the application finishes.

        Returns:
            List of dictionaries with expected output tables.
        """
        if (('storage' in self.config_data) and
                ('output' in self.config_data['storage']) and
                ('tables') in self.config_data['storage']['output']):
            tables = self.config_data['storage']['output']['tables']
            for table in tables:
                table['full_path'] = os.path.join(
                    self.data_dir,
                    'out',
                    'tables',
                    table['source']
                )
            return(tables)
        else:
            return([])

    def get_data_dir(self):
        """
        Get current working directory.

        Returns:
            String directory name.
        """
        return(self.data_dir)
