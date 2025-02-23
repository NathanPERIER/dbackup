#!/usr/bin/python3

import os
import sys
import errno
import logging
import traceback
import subprocess
from abc import ABC, abstractmethod
from typing import Any, Final

import yaml

logger = logging.getLogger(__name__)

pgpass_file = '/tmp/dbackup.pgpass'


def help(ret_code: int):
    print(f"usage: {sys.argv[0]} [-c <config_path>] [-o <output_dir>]")
    sys.exit(ret_code)


def load_yaml(path: str):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def make_empty_safe(path: str):
    open(path, 'w').close()
    os.chmod(path, 0o600)


class PgpassFile:

    def __init__(self, path: str):
        self._path: Final[str] = path
    
    def __enter__(self):
        try:
            make_empty_safe(self._path)
        except Exception:
            logger.fatal('Unable to create pgpass file')
            traceback.print_exc()
            sys.exit(1)
        return self

    def __exit__(self, *args):
        try:
            os.remove(self._path)
        except OSError as e:
            if e.errno != errno.ENOENT :
                logger.error("Error while removing pgpass file")
                traceback.print_exc()


class BackupExecutor(ABC):

    def __init__(self, name: str, conf: dict[str,Any]):
        self._name: str = name
        # TODO: support host+port
        # TODO: check that socket exists
        self._socket: str = conf['socket']
        self._user: str = conf['user']
        self._password: str = conf['password']
    
    def name(self) -> str :
        return self._name

    @abstractmethod
    def backup(self, output_dir: str):
        pass


class PostgresExecutor(BackupExecutor):

    def __init__(self, name: str, conf: dict[str,Any]):
        super().__init__(name, conf)
        with open(pgpass_file, 'a') as f:
            # TODO: needs escaping ?
            f.write(f"{self._socket}:*:*:{self._user}:{self._password}\n")

    db_list_query = 'SELECT datname FROM pg_database WHERE datname NOT LIKE \'template%\' AND has_database_privilege(CURRENT_ROLE, datname, \'connect\');'
    def get_databases(self) -> list[str] :
        proc = subprocess.run([
            'psql', '--csv', '-t', '-U', self._user, '-d', 'postgres', '-h', self._socket, '-c', PostgresExecutor.db_list_query
        ], check=True, encoding='utf-8', stdout=subprocess.PIPE)
        return proc.stdout.split()

    def backup_database(self, db_name: str, output_dir: str, format: str, extension: str):
        dump_path = os.path.join(output_dir, f"{db_name}.{extension}")
        subprocess.run([
            'pg_dump', f"--format={format}", '-U', self._user, '-h', self._socket, '-f', dump_path, db_name
        ], check=True)
        os.chmod(dump_path, 0o600)

    def backup(self, output_dir: str):
        processor_out_dir = os.path.join(output_dir, self._name)
        if not os.path.exists(processor_out_dir):
            os.mkdir(processor_out_dir)
        for db_name in self.get_databases():
            logger.info("Creating backup for database %s in %s", db_name, self._name)
            self.backup_database(db_name, processor_out_dir, 'p', 'dump')
            self.backup_database(db_name, processor_out_dir, 'c', 'pg_dump')


def load_conf(path: str) -> list[BackupExecutor] :
    data = load_yaml(path)
    res: list[BackupExecutor] = []
    for name, conf in data.items():
        # TODO: check name format
        db_type = conf['type']
        if db_type == 'postgresql' :
            res.append(PostgresExecutor(name, conf))
        # TODO: mariadb version
        # TODO: error if type is not correct
    return res


def main():
    config_path: str = ''
    output_dir: str = ''

    args = sys.argv[1:]

    if len(args) > 0 and args[0] in ['-h', '--help'] :
        help(0)

    if len(args) > 0 and args[0] == '-c' :
        if len(args) < 2 :
            help(1)
        config_path = args[1]
        args = args[2:]

    if len(args) > 0 and args[0] == '-o' :
        if len(args) < 2 :
            help(1)
        output_dir = args[1]
        args = args[2:]
    
    if len(args) > 0 :
        help(1)


    if 'DBACKUP_CONFIG_PATH' in os.environ :
        config_path = os.environ['DBACKUP_CONFIG_PATH']

    if 'DBACKUP_OUTPUT_DIR' in os.environ :
        output_dir = os.environ['DBACKUP_OUTPUT_DIR']


    if len(config_path) == 0 :
        logger.fatal('Configuration path was not specified, use the CLI or set the DBACKUP_CONFIG_PATH environment variable')
        sys.exit(1)

    if len(output_dir) == 0 :
        logger.fatal('Output directory was not specified, use the CLI or set the DBACKUP_OUTPUT_DIR environment variable')
        sys.exit(1)


    with PgpassFile(pgpass_file) :
        os.environ['PGPASSFILE'] = pgpass_file
        try:
            config = load_conf(config_path)
        except Exception:
            logger.fatal('Error during configuration parsing')
            traceback.print_exc()
            sys.exit(1)
        for executor in config :
            logger.info("Processing %s", executor.name())
            try:
                executor.backup(output_dir)
            except (Exception, subprocess.SubprocessError):
                logger.error("Error while processing backups for %s", executor.name())
                traceback.print_exc()

if __name__ == '__main__':
    main()
