"""
Module representing common interface to KBC docker applications
See docs: https://developers.keboola.com/extend/common-interface/
"""

import argparse
import json
import os
import csv


class Config(object):
    """
    Class representing configuration file and manifests generated and read
    by KBC for docker applications
    See docs:
    https://developers.keboola.com/extend/common-interface/config-file/
    and https://developers.keboola.com/extend/common-interface/manifest-files/
    """
    def __init__(self, data_dir=''):
        self.register_csv_dialect()
        self.config_data = []
        self.data_dir = ''
        if data_dir == '' or data_dir is None:
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
            if data_dir == '':
                data_dir = os.getenv('KBC_DATADIR', '')
                if data_dir == '':
                    data_dir = os.getenv('KBC_DATA_DIR', '')
                    if data_dir == '':
                        data_dir = '/data/'
        self.data_dir = data_dir
        try:
            with open(os.path.join(data_dir, 'config.json'), 'r') \
                    as config_file:
                self.config_data = json.load(config_file)
        except (OSError, IOError):
            raise ValueError(
                "Configuration file config.json not found, " +
                "verify that the data directory is correct." +
                "Dir: " + self.data_dir
            )

    @staticmethod
    def register_csv_dialect():
        """
        Register the KBC CSV dialect
        """
        csv.register_dialect('kbc', lineterminator='\n', delimiter=',',
                             quotechar='"')

    @staticmethod
    def write_file_manifest(
            file_name,
            file_tags=None,
            is_public=False,
            is_permanent=True,
            notify=False):
        """
        Write manifest for output file. Manifest is used for the file to be
        stored in KBC Storage. List with parsed configuration file structure is
        accessible as config_data property.

        Args:
            file_name: Local file name of the file to be stored,
                including path.
            file_tags: List of file tags.
            is_public: True if the file should be stored as public.
            is_permanent: False if the file should be stored only temporarily
                (for days) otherwise it will be stored until deleted.
            notify: True if members of the project should be notified
                about the file upload.
        """
        file_tags = file_tags or []
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
            destination='',
            primary_key=None,
            columns=None,
            incremental=None,
            metadata=None,
            column_metadata=None,
            delete_where=None):
        """
        Write manifest for output table Manifest is used for
        the table to be stored in KBC Storage.

        Args:
            file_name: Local file name of the CSV with table data.
            destination: String name of the table in Storage.
            primary_key: List with names of columns used for primary key.
            columns: List of columns for headless CSV files
            incremental: Set to true to enable incremental loading
            metadata: Dictionary of table metadata keys and values
            column_metadata: Dict of dict of column metadata keys and values
            delete_where: Dict with settings for deleting rows
        """
        manifest = {}
        if destination:
            if isinstance(destination, str):
                manifest['destination'] = destination
            else:
                raise TypeError("Destination must be a string")
        if primary_key:
            if isinstance(primary_key, list):
                manifest['primary_key'] = primary_key
            else:
                raise TypeError("Primary key must be a list")
        if columns:
            if isinstance(columns, list):
                manifest['columns'] = columns
            else:
                raise TypeError("Columns must by a list")
        if incremental:
            manifest['incremental'] = True
        manifest = self.process_metadata(manifest, metadata)
        manifest = self.process_column_metadata(manifest, column_metadata)
        manifest = self.process_delete(manifest, delete_where)
        with open(file_name + '.manifest', 'w') as manifest_file:
            json.dump(manifest, manifest_file)

    @staticmethod
    def process_metadata(manifest, metadata=None):
        """
        Process metadata as dictionary and returns modified manifest

        Args:
            manifest: Manifest dict
            metadata: Dictionary of table metadata keys and values

        Returns:
            Manifest dict
        """
        if metadata:
            manifest['metadata'] = []
            if isinstance(metadata, dict):
                for key in metadata:
                    manifest['metadata'].append({
                        'key': key,
                        'value': metadata[key]
                    })
            else:
                raise TypeError("Metadata must by a dict")
        return manifest

    @staticmethod
    def process_column_metadata(manifest, column_metadata=None):
        """
        Process metadata as dictionary and returns modified manifest

        Args:
            manifest: Manifest dict
            column_metadata: Dictionary of table metadata keys and values

        Returns:
            Manifest dict
        """
        if column_metadata:
            manifest['column_metadata'] = {}
            if isinstance(column_metadata, dict):
                for column in column_metadata:
                    manifest['column_metadata'][column] = []
                    if isinstance(column_metadata[column], dict):
                        for key in column_metadata[column]:
                            manifest['column_metadata'][column].append({
                                'key': key,
                                'value': column_metadata[column][key]
                            })
                    else:
                        raise TypeError("Column metadata must be a dict of "
                                        "dicts indexed by column name")
            else:
                raise TypeError("Column metadata must be a dict")
        return manifest

    @staticmethod
    def process_delete(manifest, delete_where):
        """
        Process metadata as dictionary and returns modified manifest

        Args:
            manifest: Manifest dict
            delete_where: Dictionary of where condition specification

        Returns:
            Manifest dict
        """
        if delete_where:
            if 'column' in delete_where and 'values' in delete_where:
                if not isinstance(delete_where['column'], str):
                    raise TypeError("Delete column must be a string")
                if not isinstance(delete_where['values'], list):
                    raise TypeError("Delete values must be a list")
                op = delete_where['operator'] or 'eq'
                if (not op == 'eq') and (not op == 'neq'):
                    raise ValueError("Delete operator must be 'eq' or 'neq'")
                manifest['delete_where_values'] = delete_where['values']
                manifest['delete_where_column'] = delete_where['column']
                manifest['delete_where_operator'] = op
            else:
                raise ValueError("Delete where specification must contain "
                                 "keys 'column' and 'values'")
        return manifest

    def get_parameters(self):
        """
        Get arbitrary parameters passed to the application.

        Returns:
            Dict with parameters.
        """
        if 'parameters' in self.config_data and \
                isinstance(self.config_data['parameters'], dict):
            return self.config_data['parameters']
        return {}

    def get_action(self):
        """
        Get action parameter passed to the configuration.

        Returns:
            String.
        """
        if 'action' in self.config_data:
            return self.config_data['action']
        return ''

    def get_authorization(self):
        """
        Get authorization parameters passed to the application.

        Returns:
            Dict with authorization.
        """
        if 'authorization' in self.config_data:
            return self.config_data['authorization']
        return {}

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
        if file_name[0:len(base_dir)] != base_dir:
            file_name = os.path.join(base_dir, file_name)

        with open(file_name + '.manifest') as manifest_file:
            manifest = json.load(manifest_file)
        return manifest

    def get_expected_output_files(self):
        """
        Get files which are supposed to be returned
        when the application finishes.

        Returns:
            List with dictionaries with output file properties.
        """
        if (('storage' in self.config_data) and
                ('output' in self.config_data['storage']) and
                ('files' in self.config_data['storage']['output'])):
            files = self.config_data['storage']['output']['files']
            return files
        return []

    def get_input_tables(self):
        """
        Get input tables specified in the configuration file.
        Tables are identified by their destination (.csv file) or full_path.

        Returns:
            List of dictionaries with output tables properties.
        """
        if (('storage' in self.config_data) and
                ('input' in self.config_data['storage']) and
                ('tables' in self.config_data['storage']['input'])):
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
            return tables
        return []

    def get_table_manifest(self, table_name):
        """
        Get additional table information stored in table manifest.

        Args:
            table_name: Destination table name (name of .csv file).

        Returns:
            List with manifest options.
        """
        manifest_path = os.path.join(
            self.data_dir,
            'in',
            'tables',
            table_name + '.manifest'
        )
        with open(manifest_path) as manifest_file:
            manifest = json.load(manifest_file)
        return manifest

    def get_expected_output_tables(self):
        """
        Get tables which are supposed to be returned
        when the application finishes.

        Returns:
            List of dictionaries with expected output tables.
        """
        if (('storage' in self.config_data) and
                ('output' in self.config_data['storage']) and
                ('tables' in self.config_data['storage']['output'])):
            tables = self.config_data['storage']['output']['tables']
            for table in tables:
                table['full_path'] = os.path.join(
                    self.data_dir,
                    'out',
                    'tables',
                    table['source']
                )
            return tables
        return []

    def get_data_dir(self):
        """
        Get current working directory.

        Returns:
            String directory name.
        """
        return self.data_dir

    def get_oauthapi_data(self):
        """
        Get OAuth authorization data passed to the application.

        Returns:
            Dict with oauth data.
        """
        authorization = self.get_authorization()
        if ('oauth_api' in authorization and
                'credentials' in authorization['oauth_api'] and
                '#data' in authorization['oauth_api']['credentials']):
            json_string = authorization['oauth_api']['credentials']['#data']
            return json.loads(json_string)
        return {}

    def get_oauthapi_appsecret(self):
        """
        Get application secret from OAuth authorization

        Returns:
            string
        """
        authorization = self.get_authorization()
        if ('oauth_api' in authorization and
                'credentials' in authorization['oauth_api'] and
                '#appSecret' in authorization['oauth_api']['credentials']):
            return authorization['oauth_api']['credentials']['#appSecret']
        return ''

    def get_oauthapi_appkey(self):
        """
        Get application key from OAuth authorization

        Returns:
            string
        """
        authorization = self.get_authorization()
        if ('oauth_api' in authorization and
                'credentials' in authorization['oauth_api'] and
                'appKey' in authorization['oauth_api']['credentials']):
            return authorization['oauth_api']['credentials']['appKey']
        return ''
