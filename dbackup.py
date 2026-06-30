#!/usr/bin/python3

import re
import os
import sys
import errno
import shutil
import logging
import traceback
import subprocess
from abc import ABC, abstractmethod
from time import time
from typing import Any, Final, Optional

import yaml

from metrics import MetricLabel, MetricSection

logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

pgpass_file = '/tmp/dbackup.pgpass'
name_reg = re.compile(r'[a-zA-Z0-9_-]+')


def help(ret_code: int):
    print(f"usage: {sys.argv[0]} [-c <config_path>] [-o <output_dir>] [--metrics] [--metrics-dir <path>]")
    sys.exit(ret_code)


def load_yaml(path: str):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def make_empty_safe(path: str):
    open(path, 'w').close()
    os.chmod(path, 0o600)

def file_size(path: str) -> Optional[int] :
    try:
        return os.path.getsize(path)
    except Exception as e:
        logger.warning("Unable to retrieve size for file at \"%s\": %s", e)
        return None

def dir_size(path: str) -> Optional[int] :
    # Warning: this is not recursive !
    res = 0
    try:
        for f in os.listdir(path) :
            full_path = os.path.join(path, f)
            if os.path.isfile(full_path) :
                res += os.path.getsize(full_path)
    except Exception as e:
        logger.warning("Unable to retrieve size for some file in \"%s\": %s", e)
        return None
    return res



backup_name_label = MetricLabel('backup_name')
database_name_label = MetricLabel('database_name')

class Metrics:
    def __init__(self, prefix: str):
        self._prefix: Final[str] = prefix
        self._last_backup_timestamp = MetricSection('last_run_timestamp_seconds', 'counter', 'Timestamp of the last script invocation')
        self._last_backup_timestamp.add([], time())
        self.db_backup_ok = MetricSection('database_backup_ok_bool', 'gauge', 'Value is 1 if the database backup was successful, else 0')
        self.db_backup_size = MetricSection('database_backup_size_bytes', 'gauge', 'Size of a database backup on disk')
        self.full_backup_ok = MetricSection('full_backup_ok_bool', 'gauge', 'Value is 1 if the full backup was successful, else 0')
        self.full_backup_size = MetricSection('full_backup_size_bytes', 'gauge', 'Size of a full backup on disk')

    def dump(self) -> list[str] :
        res: list[str] = []
        self._last_backup_timestamp.write(self._prefix, res)
        self.db_backup_ok.write(self._prefix, res)
        self.db_backup_size.write(self._prefix, res)
        self.full_backup_ok.write(self._prefix, res)
        self.full_backup_size.write(self._prefix, res)
        return res


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
        self._socket: str = conf['socket']
        if not os.path.exists(self._socket):
            raise FileNotFoundError(f"Socket does not exist: {self._socket}")
        self._user: str = conf['user']
        self._password: str = conf['password']
    
    def name(self) -> str :
        return self._name

    @abstractmethod
    def get_databases(self) -> list[str] :
        pass

    @abstractmethod
    def backup_database(self, db_name: str, output_dir: str) -> Optional[int] :
        pass

    def should_full_backup(self) -> bool :
        return False

    def full_backup(self, output_dir: str) -> Optional[int] :
        raise RuntimeError('Full backup is not enabled for this executor')

    def backup(self, output_dir: str, metrics: Metrics):
        processor_out_dir = os.path.join(output_dir, self._name)
        if not os.path.exists(processor_out_dir):
            os.mkdir(processor_out_dir, mode=0o700)
        labels = [backup_name_label(self._name)]
        if self.should_full_backup() :
            try:
                dump_size = self.full_backup(processor_out_dir)
                metrics.full_backup_ok.add(labels, 1)
                if dump_size is not None :
                    metrics.full_backup_size.add(labels, dump_size)
            except (Exception, subprocess.SubprocessError) as e :
                logger.error("Error while generating full backup in %s: %s", self._name, e)
                traceback.print_exc()
                metrics.full_backup_ok.add(labels, 0)
        for db_name in self.get_databases():
            db_labels = [*labels, database_name_label(db_name)]
            logger.info("Creating backup for database %s in %s", db_name, self._name)
            try:
                dump_size = self.backup_database(db_name, processor_out_dir)
                metrics.db_backup_ok.add(db_labels, 1)
                if dump_size is not None :
                    metrics.db_backup_size.add(db_labels, dump_size)
            except (Exception, subprocess.SubprocessError) as e:
                logger.error("Error while generating backup for database %s in %s: %s", db_name, self._name, e)
                traceback.print_exc()
                metrics.db_backup_ok.add(db_labels, 0)


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

    repl_perm_query = 'SELECT COUNT(*) FROM pg_user WHERE usename = CURRENT_USER AND userepl = \'t\';'
    def can_basebackup(self) -> bool :
        try:
            proc = subprocess.run([
                'psql', '--csv', '-t', '-U', self._user, '-d', 'postgres', '-h', self._socket, '-c', PostgresExecutor.repl_perm_query
            ], check=True, encoding='utf-8', stdout=subprocess.PIPE)
            return (proc.stdout.strip() == '1')
        except:
            logger.error("Error while checking replication permission")
            traceback.print_exc()
        return False

    def should_full_backup(self) -> bool :
        return True

    def full_backup(self, output_dir: str) -> Optional[int] :
        if not self.can_basebackup():
            logger.warning("Unable to perform a basebackup for %s", self._name)
            logger.info("Does the user %s have the correct permissions?", self._user)
            logger.info("(consider `ALTER USER %s REPLICATION;`)", self._user)
            raise RuntimeError('User does not have the ability to create a basebackup')
        logger.info("Creating basebackup for %s", self._name)
        basebackup_dir = os.path.join(output_dir, 'basebackup')
        if os.path.exists(basebackup_dir):
            shutil.rmtree(basebackup_dir)
        subprocess.run([
            'pg_basebackup', '-D', basebackup_dir, '--format=t', '-z', '-U', self._user, '-h', self._socket
        ], check=True)
        os.chmod(basebackup_dir, 0o700)
        return dir_size(basebackup_dir)

    def backup_database_impl(self, db_name: str, output_dir: str, format: str, extension: str) -> Optional[int] :
        dump_path = os.path.join(output_dir, f"{db_name}.{extension}")
        subprocess.run([
            'pg_dump', f"--format={format}", '-U', self._user, '-h', self._socket, '-f', dump_path, db_name
        ], check=True)
        os.chmod(dump_path, 0o600)
        return file_size(dump_path)

    def backup_database(self, db_name: str, output_dir: str) -> Optional[int] :
        size_p = self.backup_database_impl(db_name, output_dir, 'p', 'dump')
        size_c = self.backup_database_impl(db_name, output_dir, 'c', 'pg_dump')
        if size_p is None or size_c is None :
            return None
        return size_p + size_c


class MariaExecutor(BackupExecutor):

    def __init__(self, name: str, conf: dict[str,Any]):
        super().__init__(name, conf)
        self._maria_defaults: Final[str] = f"[client]\nprotocol=socket\nsocket={self._socket}\nuser={self._user}\npassword={self._password}"
    
    db_list_query = """SHOW DATABASES WHERE `Database` NOT IN ('mysql', 'performance_schema', 'information_schema', 'sys');"""
    def get_databases(self) -> list[str] :
        # mariadb-dump --defaults-file="$conf_file" "$1" > "$output_dir/$1.dump" 
        proc = subprocess.run([
            'mariadb', '--defaults-file=/dev/stdin', '--silent', '--skip-column-names', '-e', MariaExecutor.db_list_query
        ], check=True, input=self._maria_defaults, encoding='utf-8', stdout=subprocess.PIPE)
        return proc.stdout.split()

    def backup_database(self, db_name: str, output_dir: str) -> Optional[int] :
        dump_path = os.path.join(output_dir, f"{db_name}.dump")
        proc = subprocess.run([
            'mariadb-dump', '--defaults-file=/dev/stdin', db_name
        ], check=True, input=self._maria_defaults, encoding='utf-8', stdout=subprocess.PIPE)
        with open(dump_path, 'w') as f:
            f.write(proc.stdout)
        os.chmod(dump_path, 0o600)
        return file_size(dump_path)


def load_conf(path: str) -> list[BackupExecutor] :
    data: dict[str,Any] = load_yaml(path)
    res: list[BackupExecutor] = []
    for name, conf in data.items():
        if not name_reg.fullmatch(name):
            raise ValueError(f"Bad backup name: {name}")
        db_type = conf['type']
        if db_type == 'postgresql' :
            res.append(PostgresExecutor(name, conf))
        elif db_type == 'mariadb' :
            res.append(MariaExecutor(name, conf))
        else:
            raise ValueError(f"Bad database type: {db_type}")
    return res


def main():
    config_path: str = ''
    output_dir: str = ''
    write_metrics = False
    metrics_dir = '/var/lib/node_exporter/textfile_collector'

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

    if len(args) > 0 and args[0] == '--metrics' :
        write_metrics = True
        args = args[1:]

    if len(args) > 0 and args[0] == '--metrics-dir' :
        if len(args) < 2 :
            help(1)
        metrics_dir = args[1]
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


    metrics = Metrics('dbackup')

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
                executor.backup(output_dir, metrics)
            except Exception:
                logger.error("Unexpected error while processing backups for %s", executor.name())
                traceback.print_exc()

    if write_metrics:
        metrics_file = os.path.join(metrics_dir, 'dbackup.prom')
        metrics_tmp_file = f"{metrics_file}.tmp"
        try:
            with open(metrics_tmp_file, 'w') as f:
                f.write("\n".join(metrics.dump()))
            # Atomic write
            os.rename(metrics_tmp_file, metrics_file)
        except Exception:
            logger.error("Error while writing metrics")
            traceback.print_exc()

if __name__ == '__main__':
    main()
