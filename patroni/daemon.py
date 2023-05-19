"""Daemon processes abstraction module.

This module implements abstraction classes and functions for creating and managing daemon processes in Patroni.
Currently it is only used for the main "Thread" of ``patroni`` and ``patroni_raft_controller`` commands.
"""
from __future__ import print_function

import abc
import os
import signal
import socket
import sys
import yaml

from threading import Lock
from typing import Any, Dict, Optional, Type, TYPE_CHECKING

from .exceptions import PatroniException
from .postgresql.misc import postgres_major_version_to_int


if TYPE_CHECKING:  # pragma: no cover
    from .config import Config
    from .validator import Schema


def get_bin_dir_from_running_instance(data_dir: str) -> str:
    postmaster_pid = None
    try:
        with open(f"{data_dir}/postmaster.pid", 'r') as f:
            postmaster_pid = f.readline()
            if not postmaster_pid:
                print('Failed to obtain postmaster pid from postmaster.pid file', file=sys.stderr)
                sys.exit(1)
            postmaster_pid = int(postmaster_pid.strip())
    except OSError as e:
        print(f'Error while reading postmaster.pid file: {e}', file=sys.stderr)
        sys.exit(1)
    import psutil
    try:
        return os.path.dirname(psutil.Process(postmaster_pid).exe())
    except psutil.NoSuchProcess:
        print('Obtained postmaster pid doesn\'t exist', file=sys.stderr)
        sys.exit(1)


def enrich_config_from_running_instance(config: Dict[str, Any], no_value_msg: str, dsn: Optional[str] = None) -> None:
    """Get
    - non-internal GUC values having configuration file, postmaster command line or environment variable as a source
    - postgresql.connect_address, postgresql.listen,
    - postgresql.pg_hba and postgresql.pg_ident
    - superuser auth parameters (from the options used for connection)
    And redefine scope with the clister_name GUC value if set

    :param config: configuration parameters dict to be enriched
    :param no_value_msg: str value to be used when a parameter value is not available
    :param dsn: optional DSN string for the source running instance
    """
    from getpass import getuser, getpass
    from patroni.postgresql.config import parse_dsn
    from patroni.config import AUTH_ALLOWED_PARAMETERS_MAPPING

    def get_local_ip() -> str:
        patroni_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            patroni_socket.connect(('8.8.8.8', 80))
            ip = patroni_socket.getsockname()[0]
        except OSError as e:
            print(f'Failed to define local ip: {e}', file=sys.stderr)
            sys.exit(1)
        finally:
            patroni_socket.close()
        return ip

    su_params: Dict[str, str] = {}
    parsed_dsn = {}

    if dsn:
        parsed_dsn = parse_dsn(dsn)
        if not parsed_dsn:
            print('Failed to parse DSN string', file=sys.stderr)
            sys.exit(1)

    # gather auth parameters for the superuser config
    for conn_param, env_var in AUTH_ALLOWED_PARAMETERS_MAPPING.items():
        val = parsed_dsn.get(conn_param, os.getenv(env_var))
        if val:
            su_params[conn_param] = val
    # because we use "username" in the config for some reason
    su_params['username'] = su_params.pop('user', getuser())
    su_params['password'] = su_params.get('password') or getpass('Please enter the user password:')

    from . import psycopg
    try:
        conn = psycopg.connect(dsn=dsn, password=su_params['password'])
    except psycopg.Error as e:
        print(f'Failed to establish PostgreSQL connection: {e}', file=sys.stderr)
        sys.exit(1)

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s AND rolsuper='t';", (su_params['username'],))
        if cur.rowcount < 1:
            print('The provided user does not have superuser privilege', file=sys.stderr)
            sys.exit(1)

        cur.execute("SELECT name, current_setting(name) FROM pg_settings \
                     WHERE context <> 'internal' \
                     AND source IN ('configuration file', 'command line', 'environment variable') \
                     AND category <> 'Write-Ahead Log / Recovery Target' \
                     AND setting <> '(disabled)' \
                     OR name IN ('hba_file', 'ident_file', 'config_file', \
                                 'data_directory', \
                                 'listen_addresses', 'port', \
                                 'max_connections', 'max_worker_processes', 'max_wal_senders', \
                                 'max_replication_slots', 'max_locks_per_transaction', 'max_prepared_transactions', \
                                 'hot_standby', \
                                 'wal_level', 'wal_log_hints', \
                                 'wal_keep_segments', 'wal_keep_size', \
                                 'track_commit_timestamp');")

        # adjust values
        for p, v in cur.fetchall():
            if p == 'data_directory':
                config['postgresql']['data_dir'] = v
            elif p == 'cluster_name':
                config['scope'] = v
            elif p in ('archive_command', 'restore_command', 'archive_cleanup_command',
                       'recovery_end_command', 'ssl_passphrase_command',
                       'hba_file', 'ident_file', 'config_file'):
                # write commands to the local config due to security implications
                # write hba/ident/config_file to local config to ensure they are not removed later
                config['postgresql'].setdefault('parameters', {})
                config['postgresql']['parameters'][p] = v
            else:
                config['bootstrap']['dcs']['postgresql']['parameters'][p] = v

    conn.close()

    port = config['bootstrap']['dcs']['postgresql']['parameters']['port']
    connect_host = parsed_dsn.get('host', os.getenv('PGHOST', get_local_ip()))
    connect_port = parsed_dsn.get('port', os.getenv('PGPORT', port))
    config['postgresql']['connect_address'] = f'{connect_host}:{connect_port}'
    listen_addresses = config['bootstrap']['dcs']['postgresql']['parameters']['listen_addresses']
    config['postgresql']['listen'] = f'{listen_addresses}:{port}'

    try:
        with open(f"{config['postgresql']['parameters']['hba_file']}", 'r') as f:
            config['postgresql']['pg_hba'] = [i.strip() for i in f.readlines()
                                              if i.startswith(('local',
                                                               'host',
                                                               'hostssl',
                                                               'hostnossl',
                                                               'hostgssenc',
                                                               'hostnogssenc'))]
    except OSError as e:
        print(f'Failed to read hba_file: {e}', file=sys.stderr)
        sys.exit(1)

    try:
        with open(f"{config['postgresql']['parameters']['ident_file']}", 'r') as f:
            config['postgresql']['pg_ident'] = [i.strip() for i in f.readlines() if i.strip() and not i.startswith('#')]
    except OSError as e:
        print(f'Failed to read ident_file: {e}', file=sys.stderr)
        sys.exit(1)

    config['postgresql']['authentication'] = {
        'superuser': su_params,
        'replication': {'username': no_value_msg, 'password': no_value_msg}
    }


def generate_config(file: str, sample: bool, dsn: Optional[str]) -> None:
    """Generate Patroni configuration file

    Gather all the available non-internal GUC values having configuration file, postmaster command line or environment
    variable as a source and store them in the appropriate part of Patroni configuration (``postgresql.parameters`` or
    ``bootsrtap.dcs.postgresql.parameters``). Either the provided DSN (takes precedence) or PG ENV vars will be used
    for the connection. If password is not provided, it should be entered via prompt.

    The created configuration contains:
    - ``scope``: cluster_name GUC value or PATRONI_SCOPE ENV variable value if available
    - ``name``: PATRONI_NAME ENV variable value if set, otherewise hostname
    - ``bootsrtap.dcs``: section with all the parameters (incl. the majority of PG GUCs) set to their default values
      defined by Patroni and adjusted by the source instances's configuration values.
    - ``postgresql.parameters``: the source instance's archive_command, restore_command, archive_cleanup_command,
      recovery_end_command, ssl_passphrase_command, hba_file, ident_file, config_file GUC values
    - ``postgresql.bin_dir``: path to Postgres binaries gathered from the running instance or, if not available,
      the value of PATRONI_POSTGRESQL_BIN_DIR ENV variable. Otherwise, an empty string.
    - ``postgresql.datadir``: the value gathered from the corresponding PG GUC
    - ``postgresql.listen``: source instance's listen_addresses and port GUC values
    - ``postgresql.connect_address``: if possible, generated from the connection params
    - ``postgresql.authentication``:
        - superuser and replication users defined (if possible, usernames are set from the respective Patroni ENV vars,
          otherwise the default 'postgres' and 'replicator' values are used).
          If not a sample config, either DSN or PG ENV vars are used to define superuser authentication parameters.
        - rewind user is defined for a sample config if PG version can be defined and PG version is 11+
          (if possible, username is set from the respective Patroni ENV var)
    - ``bootsrtap.dcs.postgresql.use_pg_rewind set to True if PG version is 11+
    - ``postgresql.pg_hba`` defaults or the lines gathered from the source instance's hba_file
    - ``postgresql.pg_ident`` the lines gathered from the source instance's ident_file

    :param file: Full path to the configuration file to be created (/tmp/patroni.yml by default).
    :param sample: Optional flag. If set, no source instance will be used - generate config with some sane defaults.
    :param dsn: Optional DSN string for the local instance to get GUC values from.
    """
    from patroni.config import Config
    from patroni.validator import get_major_version

    no_value_msg = '#FIXME'
    pg_version = None

    dynamic_config = Config.get_default_config()
    dynamic_config['postgresql']['parameters'] = dict(dynamic_config['postgresql']['parameters'])
    config: Dict[str, Any] = {
        'scope': os.getenv('PATRONI_SCOPE', no_value_msg),
        'name': os.getenv('PATRONI_NAME') or socket.gethostname(),
        'bootstrap': {
            'dcs': dynamic_config
        },
        'postgresql': {
            'data_dir': no_value_msg,
            'connect_address': no_value_msg,
            'listen': no_value_msg,
        },
    }

    if not sample:
        enrich_config_from_running_instance(config, no_value_msg, dsn)

    bin_dir = os.getenv('PATRONI_POSTGRESQL_BIN_DIR', '')
    config['postgresql']['bin_dir'] = bin_dir
    if not sample:
        config['postgresql']['bin_dir'] = get_bin_dir_from_running_instance(config['postgresql']['data_dir'])

    # obtain version from the binary
    try:
        pg_version = postgres_major_version_to_int(get_major_version(config['postgresql'].get('bin_dir') or None))
    except PatroniException as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    # generate sample config
    if sample:
        # some sane defaults or values set via Patroni env vars
        replicator = os.getenv('PATRONI_REPLICATION_USERNAME', 'replicator')
        config['postgresql']['authentication'] = {
            'superuser': {
                'username': os.getenv('PATRONI_SUPERUSER_USERNAME', 'postgres'),
                'password': os.getenv('PATRONI_SUPERUSER_PASSWORD', no_value_msg)
            },
            'replication': {
                'username': replicator,
                'password': os.getenv('PATRONI_REPLICATION_PASSWORD', no_value_msg)
            }
        }

        auth_method = 'scram-sha-256' if pg_version and pg_version >= 100000 else 'md5'
        config['postgresql']['pg_hba'] = [
            f'host all all all {auth_method}',
            f'host replication {replicator} all {auth_method}'
        ]

        # add version-specific configuration
        if pg_version:
            from patroni.postgresql.config import ConfigHandler
            wal_keep_param = 'wal_keep_segments' if pg_version < 130000 else 'wal_keep_size'
            config['bootstrap']['dcs']['postgresql']['parameters'][wal_keep_param] =\
                ConfigHandler.CMDLINE_OPTIONS[wal_keep_param][0]

            if pg_version >= 110000:
                config['bootstrap']['dcs']['postgresql']['use_pg_rewind'] = True
                config['postgresql']['authentication']['rewind'] = {
                    'username': os.getenv('PATRONI_REWIND_USERNAME', 'rewind_user'),
                    'password': no_value_msg
                }

    # redundant values from the default config
    del config['bootstrap']['dcs']['postgresql']['parameters']['listen_addresses']
    del config['bootstrap']['dcs']['postgresql']['parameters']['port']
    del config['bootstrap']['dcs']['postgresql']['parameters']['cluster_name']
    del config['bootstrap']['dcs']['standby_cluster']

    dir_path = os.path.dirname(file)
    if dir_path and not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    with open(file, 'w') as fd:
        yaml.safe_dump(config, fd, default_flow_style=False)


class AbstractPatroniDaemon(abc.ABC):
    """A Patroni daemon process.

    .. note::

        When inheriting from :class:`AbstractPatroniDaemon` you are expected to define the methods :func:`_run_cycle`
        to determine what it should do in each execution cycle, and :func:`_shutdown` to determine what it should do
        when shutting down.

    :ivar logger: log handler used by this daemon.
    :ivar config: configuration options for this daemon.
    """

    def __init__(self, config: 'Config') -> None:
        """Set up signal handlers, logging handler and configuration.

        :param config: configuration options for this daemon.
        """
        from patroni.log import PatroniLogger

        self.setup_signal_handlers()

        self.logger = PatroniLogger()
        self.config = config
        AbstractPatroniDaemon.reload_config(self, local=True)

    def sighup_handler(self, *_: Any) -> None:
        """Handle SIGHUP signals.

        Flag the daemon as "SIGHUP received".
        """
        self._received_sighup = True

    def api_sigterm(self) -> bool:
        """Guarantee only a single SIGTERM is being processed.

        Flag the daemon as "SIGTERM received" with a lock-based approach.

        :returns: ``True`` if the daemon was flagged as "SIGTERM received".
        """
        ret = False
        with self._sigterm_lock:
            if not self._received_sigterm:
                self._received_sigterm = True
                ret = True
        return ret

    def sigterm_handler(self, *_: Any) -> None:
        """Handle SIGTERM signals.

        Terminate the daemon process through :func:`api_sigterm`.
        """
        if self.api_sigterm():
            sys.exit()

    def setup_signal_handlers(self) -> None:
        """Set up daemon signal handlers.

        Set up SIGHUP and SIGTERM signal handlers.

        .. note::

            SIGHUP is only handled in non-Windows environments.
        """
        self._received_sighup = False
        self._sigterm_lock = Lock()
        self._received_sigterm = False
        if os.name != 'nt':
            signal.signal(signal.SIGHUP, self.sighup_handler)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    @property
    def received_sigterm(self) -> bool:
        """If daemon was signaled with SIGTERM."""
        with self._sigterm_lock:
            return self._received_sigterm

    def reload_config(self, sighup: bool = False, local: Optional[bool] = False) -> None:
        """Reload configuration.

        :param sighup: if it is related to a SIGHUP signal.
                       The sighup parameter could be used in the method overridden in a child class.
        :param local: will be ``True`` if there are changes in the local configuration file.
        """
        if local:
            self.logger.reload_config(self.config.get('log', {}))

    @abc.abstractmethod
    def _run_cycle(self) -> None:
        """Define what the daemon should do in each execution cycle.

        Keep being called in the daemon's main loop until the daemon is eventually terminated.
        """

    def run(self) -> None:
        """Run the daemon process.

        Start the logger thread and keep running execution cycles until a SIGTERM is eventually received. Also reload
        configuration uppon receiving SIGHUP.
        """
        self.logger.start()
        while not self.received_sigterm:
            if self._received_sighup:
                self._received_sighup = False
                self.reload_config(True, self.config.reload_local_configuration())

            self._run_cycle()

    @abc.abstractmethod
    def _shutdown(self) -> None:
        """Define what the daemon should do when shutting down."""

    def shutdown(self) -> None:
        """Shut the daemon down when a SIGTERM is received.

        Shut down the daemon process and the logger thread.
        """
        with self._sigterm_lock:
            self._received_sigterm = True
        self._shutdown()
        self.logger.shutdown()


def abstract_main(cls: Type[AbstractPatroniDaemon], validator: Optional['Schema'] = None) -> None:
    """Create the main entry point of a given daemon process.

    Expose a basic argument parser, parse the command-line arguments, and run the given daemon process.

    :param cls: a class that should inherit from :class:`AbstractPatroniDaemon`.
    :param validator: used to validate the daemon configuration schema, if requested by the user through
        ``--validate-config`` CLI option.
    """
    import argparse

    from .config import Config, ConfigParseError
    from .version import __version__

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version', version='%(prog)s {0}'.format(__version__))
    if validator:
        parser.add_argument('--validate-config', action='store_true', help='Run config validator and exit')
    parser.add_argument('--generate-sample-config', action='store_true',
                        help='Generate a sample Patroni yaml configuration file')
    parser.add_argument('--generate-config', action='store_true',
                        help='Generate a Patroni yaml configuration file for a running instance')
    parser.add_argument('--dsn', help='Optional DSN string of the instance to be used as a source \
                                       for config generation. Superuser connection is required.')
    parser.add_argument('configfile', nargs='?', default='',
                        help='Patroni may also read the configuration from the {0} environment variable'
                        .format(Config.PATRONI_CONFIG_VARIABLE))
    args = parser.parse_args()

    if args.generate_sample_config:
        generate_config(args.configfile or '/tmp/patroni.yml', True, None)
        sys.exit(0)
    elif args.generate_config:
        generate_config(args.configfile or '/tmp/patroni.yml', False, args.dsn)
        sys.exit(0)

    validate_config = validator and args.validate_config
    try:
        if validate_config:
            Config(args.configfile, validator=validator)
            sys.exit()

        config = Config(args.configfile)
    except ConfigParseError as e:
        if e.value:
            print(e.value, file=sys.stderr)
        if not validate_config:
            parser.print_help()
        sys.exit(1)

    controller = cls(config)
    try:
        controller.run()
    except KeyboardInterrupt:
        pass
    finally:
        controller.shutdown()
