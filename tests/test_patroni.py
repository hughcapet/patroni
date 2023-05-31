import etcd
import logging
import os
import psutil
import signal
import time
import unittest

from . import MockCursor
import patroni.config as config
from http.server import HTTPServer
from mock import MagicMock, Mock, PropertyMock, mock_open, patch
from patroni.api import RestApiServer
from patroni.async_executor import AsyncExecutor
from patroni.dcs.etcd import AbstractEtcdClientWithFailover
from patroni.config import Config
from patroni.exceptions import DCSError
from patroni.postgresql import Postgresql
from patroni.postgresql.config import ConfigHandler
from patroni import check_psycopg
from patroni.__main__ import Patroni, main as _main
from threading import Thread

from . import psycopg_connect, SleepException
from .test_etcd import etcd_read, etcd_write
from .test_postgresql import MockPostmaster


def mock_import(*args, **kwargs):
    if args[0] == 'psycopg':
        raise ImportError
    ret = Mock()
    ret.__version__ = '2.5.3.dev1 a b c'
    return ret


class MockFrozenImporter(object):

    toc = set(['patroni.dcs.etcd'])


@patch('time.sleep', Mock())
@patch('subprocess.call', Mock(return_value=0))
@patch('patroni.psycopg.connect', psycopg_connect)
@patch.object(ConfigHandler, 'append_pg_hba', Mock())
@patch.object(ConfigHandler, 'write_postgresql_conf', Mock())
@patch.object(ConfigHandler, 'write_recovery_conf', Mock())
@patch.object(Postgresql, 'is_running', Mock(return_value=MockPostmaster()))
@patch.object(Postgresql, 'call_nowait', Mock())
@patch.object(HTTPServer, '__init__', Mock())
@patch.object(AsyncExecutor, 'run', Mock())
@patch.object(etcd.Client, 'write', etcd_write)
@patch.object(etcd.Client, 'read', etcd_read)
class TestPatroni(unittest.TestCase):

    @patch('sys.argv', ['patroni.py'])
    def test_no_config(self):
        self.assertRaises(SystemExit, _main)

    @patch('sys.argv', ['patroni.py', '--validate-config', 'postgres0.yml'])
    @patch('socket.socket.connect_ex', Mock(return_value=1))
    def test_validate_config(self):
        self.assertRaises(SystemExit, _main)
        with patch.object(config.Config, '__init__', Mock(return_value=None)):
            self.assertRaises(SystemExit, _main)

    @patch('patroni.psycopg.connect', psycopg_connect)
    @patch('socket.getaddrinfo', Mock(return_value=[(0, 0, 0, 0, ('1.9.8.4', 1984))]))
    @patch('builtins.open', MagicMock())
    @patch('socket.gethostname', Mock(return_value='testhost'))
    @patch('os.makedirs')
    @patch('yaml.safe_dump')
    def test_generate_sample_config(self, mock_config_dump, mock_makedir):
        self.maxDiff = None
        os.environ['PATRONI_SCOPE'] = 'scope_from_env'
        os.environ['PATRONI_POSTGRESQL_BIN_DIR'] = '/bin/from/env'
        os.environ['PATRONI_SUPERUSER_USERNAME'] = 'su_user_from_env'
        os.environ['PATRONI_SUPERUSER_PASSWORD'] = 'su_pwd_from_env'
        os.environ['PATRONI_REPLICATION_USERNAME'] = 'repl_user_from_env'
        os.environ['PATRONI_REPLICATION_PASSWORD'] = 'repl_pwd_from_env'
        os.environ['PATRONI_REWIND_USERNAME'] = 'rewind_user_from_env'
        os.environ['PGUSER'] = 'pguser_from_env'
        os.environ['PGPASSWORD'] = 'pguser_pwd_from_env'
        os.environ['PATRONI_RESTAPI_CONNECT_ADDRESS'] = 'localhost:8080'
        os.environ['PATRONI_RESTAPI_LISTEN'] = 'localhost:8080'

        # 1. Wrong input
        # 1.1 Wrong DSN format
        with patch('sys.argv', ['patroni.py', '--generate-config', '--dsn', 'host:foo port:bar user:foobar']),\
             self.assertRaises(SystemExit) as e:
            _main()
        self.assertIn('Failed to parse DSN string', e.exception.code)

        # 1.2 User is not a superuser
        with patch('sys.argv', ['patroni.py',
                                '--generate-config', '--dsn', 'host=foo port=bar user=foobar password=pwd_from_dsn']),\
             patch.object(MockCursor, 'rowcount', PropertyMock(return_value=0), create=True),\
             self.assertRaises(SystemExit) as e:
            _main()
        self.assertIn('The provided user does not have superuser privilege', e.exception.code)

        no_value_msg = '#FIXME'
        dynamic_config = Config.get_default_config()
        dynamic_config['postgresql']['parameters'] = dict(dynamic_config['postgresql']['parameters'])
        del dynamic_config['standby_cluster']
        dynamic_config['postgresql']['parameters']['wal_keep_segments'] = 8
        dynamic_config['postgresql']['use_pg_rewind'] = True

        config = {
            'scope': os.environ['PATRONI_SCOPE'],
            'name': 'testhost',
            'bootstrap': {
                'dcs': dynamic_config
            },
            'postgresql': {
                'connect_address': no_value_msg,
                'data_dir': no_value_msg,
                'listen': no_value_msg,
                'pg_hba': ['host all all all md5',
                           f'host replication {os.environ["PATRONI_REPLICATION_USERNAME"]} all md5'],
                'authentication': {'superuser': {'username': os.environ['PATRONI_SUPERUSER_USERNAME'],
                                                 'password': os.environ['PATRONI_SUPERUSER_PASSWORD']},
                                   'replication': {'username': os.environ["PATRONI_REPLICATION_USERNAME"],
                                                   'password': os.environ['PATRONI_REPLICATION_PASSWORD']}},
                'bin_dir': os.environ['PATRONI_POSTGRESQL_BIN_DIR'],
                'parameters': {'password_encryption': 'md5'}
            },
            'restapi': {
                'connect_address': os.environ['PATRONI_RESTAPI_CONNECT_ADDRESS'],
                'listen': os.environ['PATRONI_RESTAPI_LISTEN']
            }
        }

        # 2. Sample config

        # Version-specific params
        # 2.1 pg_version < 13, with dir creation
        with patch('sys.argv', ['patroni.py', '--generate-sample-config', '/foo/bar.yml']),\
             patch('subprocess.check_output', Mock(return_value=b"postgres (PostgreSQL) 9.4.3")),\
             self.assertRaises(SystemExit) as e:
            _main()
        self.assertEqual(e.exception.code, 0)
        self.assertEqual(config, mock_config_dump.call_args[0][0])
        mock_makedir.assert_called_once()

        mock_makedir.reset_mock()
        mock_config_dump.reset_mock()

        # 2.2 pg_version >= 13
        del config['bootstrap']['dcs']['postgresql']['parameters']['wal_keep_segments']
        config['bootstrap']['dcs']['postgresql']['parameters']['wal_keep_size'] = '128MB'
        config['postgresql']['authentication']['rewind'] = {'username': os.environ['PATRONI_REWIND_USERNAME'],
                                                            'password': no_value_msg}
        config['postgresql']['parameters']['password_encryption'] = 'scram-sha-256'
        config['postgresql']['pg_hba'] = \
            ['host all all all scram-sha-256',
             f'host replication {os.environ["PATRONI_REPLICATION_USERNAME"]} all scram-sha-256']
        with patch('sys.argv', ['patroni.py', '--generate-sample-config', '/foo/bar.yml']),\
             patch('subprocess.check_output', Mock(return_value=b"postgres (PostgreSQL) 15.2")),\
             self.assertRaises(SystemExit) as e:
            _main()
        self.assertEqual(e.exception.code, 0)
        self.assertEqual(config, mock_config_dump.call_args[0][0])

        mock_config_dump.reset_mock()

        # 3. Config for a running PG13+ instance
        del config['bootstrap']['dcs']['postgresql']['parameters']['wal_keep_size']

        # values are taken from tests/__init__.py
        config['scope'] = 'my_cluster'  # cluster_name
        config['postgresql']['connect_address'] = '1.9.8.4:bar'
        config['postgresql']['listen'] = '6.6.6.6:1984'
        config['postgresql']['parameters'] = {'archive_command': 'my archive command'}
        config['postgresql']['parameters']['hba_file'] = os.path.join('data', 'pg_hba.conf')
        config['postgresql']['parameters']['ident_file'] = os.path.join('data', 'pg_ident.conf')
        config['bootstrap']['dcs']['postgresql']['parameters']['max_connections'] = 42
        config['bootstrap']['dcs']['postgresql']['parameters']['max_locks_per_transaction'] = 73
        config['bootstrap']['dcs']['postgresql']['parameters']['max_replication_slots'] = 21
        config['bootstrap']['dcs']['postgresql']['parameters']['max_wal_senders'] = 37
        config['bootstrap']['dcs']['postgresql']['parameters']['wal_level'] = 'replica'
        config['postgresql']['data_dir'] = 'data'
        config['postgresql']['bin_dir'] = '/bin/dir/from/running'
        config['postgresql']['authentication']['superuser'] = {
            'username': 'foobar',
            'password': 'qwerty',
            'channel_binding': 'prefer',
            'gssencmode': 'prefer',
            'sslmode': 'prefer'
        }
        config['postgresql']['authentication']['replication'] = {'username': no_value_msg, 'password': no_value_msg}

        # Rewind is not configured for a running instance config
        del config['bootstrap']['dcs']['postgresql']['use_pg_rewind']
        del config['postgresql']['authentication']['rewind']

        hba_content = '\n'.join(config['postgresql']['pg_hba'] + ['#host all all all md5'])
        ident_content = '\n'.join(['# something very interesting', '  '])
        open_res = []
        for _ in range(3):
            open_res.extend([
                mock_open(read_data=hba_content)(),
                mock_open(read_data=ident_content)(),
                mock_open(read_data='1984')(),
                mock_open()()
            ])

        with patch('psutil.Process.__init__', Mock(return_value=None)),\
             patch('psutil.Process.exe', Mock(return_value='/bin/dir/from/running/postgres')),\
             patch('subprocess.check_output', Mock(return_value=b"postgres (PostgreSQL) 15.2")),\
             patch('builtins.open', Mock(side_effect=open_res)):

            # 3.1.1 pg_version >= 13
            with patch('sys.argv', ['patroni.py', '--generate-config',
                                    '--dsn', 'host=foo port=bar user=foobar password=qwerty']),\
                 self.assertRaises(SystemExit) as e:
                _main()
            self.assertEqual(e.exception.code, 0)
            self.assertEqual(config, mock_config_dump.call_args[0][0])

            mock_config_dump.reset_mock()

            # 3.1.2 su auth params and connect host from env
            os.environ['PGCHANNELBINDING'] =\
                config['postgresql']['authentication']['superuser']['channel_binding'] = 'disable'
            del config['postgresql']['authentication']['superuser']['gssencmode']
            del config['postgresql']['authentication']['superuser']['sslmode']
            config['postgresql']['authentication']['superuser']['username'] = os.environ['PGUSER']
            config['postgresql']['authentication']['superuser']['password'] = os.environ['PGPASSWORD']
            config['postgresql']['connect_address'] = '1.9.8.4:1984'

            with patch('sys.argv', ['patroni.py', '--generate-config']):

                with self.assertRaises(SystemExit) as e:
                    _main()

                self.assertEqual(config, mock_config_dump.call_args[0][0])

                mock_config_dump.reset_mock()

                # 3.1.5 Error while calling postgres --version
                with patch('subprocess.check_output', Mock(side_effect=OSError)),\
                     self.assertRaises(SystemExit) as e:
                    _main()
                self.assertIn('Failed to get postgres version:', e.exception.code)

                with patch('builtins.open', Mock(side_effect=[mock_open(read_data=hba_content)(),
                                                              mock_open(read_data=ident_content)(),
                                                              mock_open(read_data='')(),  # 3.2 empty postmaster.pid
                                                              mock_open(read_data=hba_content)(),
                                                              mock_open(read_data=ident_content)(),
                                                              OSError,  # 3.3 Failed to open postmaster.pid
                                                              mock_open(read_data=hba_content)(),
                                                              mock_open(read_data=ident_content)(),
                                                              # 3.4 Invalid postmaster pid
                                                              mock_open(read_data='1984')()
                                                              ])):
                    with self.assertRaises(SystemExit) as e:
                        _main()
                    self.assertIn('Failed to obtain postmaster pid from postmaster.pid file', e.exception.code)

                    with self.assertRaises(SystemExit) as e:
                        _main()
                    self.assertIn('Error while reading postmaster.pid file', e.exception.code)

                    with patch('psutil.Process.__init__', Mock(return_value=None)),\
                         patch('psutil.Process.exe', Mock(side_effect=psutil.NoSuchProcess(1984))),\
                         self.assertRaises(SystemExit) as e:
                        _main()
                    self.assertIn("Obtained postmaster pid doesn't exist", e.exception.code)

                # 3.5 Failed to open pg_hba
                with patch('builtins.open', Mock(side_effect=OSError)),\
                     self.assertRaises(SystemExit) as e:
                    _main()
                self.assertIn('Failed to read pg_hba.conf', e.exception.code)

                # 3.6 Failed to open pg_ident
                with patch('builtins.open', Mock(side_effect=[mock_open(read_data=hba_content)(), OSError])),\
                     self.assertRaises(SystemExit) as e:
                    _main()
                self.assertIn('Failed to read pg_ident.conf', e.exception.code)

                # 3.7 Failed PG connecttion
                from . import psycopg
                with patch('patroni.psycopg.connect', side_effect=psycopg.Error),\
                     self.assertRaises(SystemExit) as e:
                    _main()
                self.assertIn('Failed to establish PostgreSQL connection', e.exception.code)

                # 3.8 Failed to get local IP
                with patch('socket.getaddrinfo', Mock(side_effect=[OSError, []])):
                    with self.assertRaises(SystemExit) as e:
                        _main()
                    self.assertIn('Failed to define ip address', e.exception.code)
                    with self.assertRaises(SystemExit) as e:
                        _main()
                    self.assertIn('Failed to define ip address. No address returned by getaddrinfo for',
                                  e.exception.code)

    @patch('pkgutil.iter_importers', Mock(return_value=[MockFrozenImporter()]))
    @patch('sys.frozen', Mock(return_value=True), create=True)
    @patch.object(HTTPServer, '__init__', Mock())
    @patch.object(etcd.Client, 'read', etcd_read)
    @patch.object(Thread, 'start', Mock())
    @patch.object(AbstractEtcdClientWithFailover, '_get_machines_list', Mock(return_value=['http://remotehost:2379']))
    def setUp(self):
        self._handlers = logging.getLogger().handlers[:]
        RestApiServer._BaseServer__is_shut_down = Mock()
        RestApiServer._BaseServer__shutdown_request = True
        RestApiServer.socket = 0
        os.environ['PATRONI_POSTGRESQL_DATA_DIR'] = 'data/test0'
        conf = config.Config('postgres0.yml')
        self.p = Patroni(conf)

    def tearDown(self):
        logging.getLogger().handlers[:] = self._handlers

    @patch('patroni.dcs.AbstractDCS.get_cluster', Mock(side_effect=[None, DCSError('foo'), None]))
    def test_load_dynamic_configuration(self):
        self.p.config._dynamic_configuration = {}
        self.p.load_dynamic_configuration()
        self.p.load_dynamic_configuration()

    @patch('sys.argv', ['patroni.py', 'postgres0.yml'])
    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch.object(etcd.Client, 'delete', Mock())
    @patch.object(AbstractEtcdClientWithFailover, '_get_machines_list', Mock(return_value=['http://remotehost:2379']))
    @patch.object(Thread, 'join', Mock())
    def test_patroni_patroni_main(self):
        with patch('subprocess.call', Mock(return_value=1)):
            with patch.object(Patroni, 'run', Mock(side_effect=SleepException)):
                os.environ['PATRONI_POSTGRESQL_DATA_DIR'] = 'data/test0'
                self.assertRaises(SleepException, _main)
            with patch.object(Patroni, 'run', Mock(side_effect=KeyboardInterrupt())):
                with patch('patroni.ha.Ha.is_paused', Mock(return_value=True)):
                    os.environ['PATRONI_POSTGRESQL_DATA_DIR'] = 'data/test0'
                    _main()

    @patch('os.getpid')
    @patch('multiprocessing.Process')
    @patch('patroni.__main__.patroni_main', Mock())
    def test_patroni_main(self, mock_process, mock_getpid):
        mock_getpid.return_value = 2
        _main()

        mock_getpid.return_value = 1

        def mock_signal(signo, handler):
            handler(signo, None)

        with patch('signal.signal', mock_signal):
            with patch('os.waitpid', Mock(side_effect=[(1, 0), (0, 0)])):
                _main()
            with patch('os.waitpid', Mock(side_effect=OSError)):
                _main()

        ref = {'passtochild': lambda signo, stack_frame: 0}

        def mock_sighup(signo, handler):
            if hasattr(signal, 'SIGHUP') and signo == signal.SIGHUP:
                ref['passtochild'] = handler

        def mock_join():
            ref['passtochild'](0, None)

        mock_process.return_value.join = mock_join
        with patch('signal.signal', mock_sighup), patch('os.kill', Mock()):
            self.assertIsNone(_main())

    @patch('patroni.config.Config.save_cache', Mock())
    @patch('patroni.config.Config.reload_local_configuration', Mock(return_value=True))
    @patch('patroni.ha.Ha.is_leader', Mock(return_value=True))
    @patch.object(Postgresql, 'state', PropertyMock(return_value='running'))
    @patch.object(Postgresql, 'data_directory_empty', Mock(return_value=False))
    def test_run(self):
        self.p.postgresql.set_role('replica')
        self.p.sighup_handler()
        self.p.ha.dcs.watch = Mock(side_effect=SleepException)
        self.p.api.start = Mock()
        self.p.logger.start = Mock()
        self.p.config._dynamic_configuration = {}
        with patch('patroni.dcs.Cluster.is_unlocked', Mock(return_value=True)):
            self.assertRaises(SleepException, self.p.run)
        with patch('patroni.config.Config.reload_local_configuration', Mock(return_value=False)):
            self.p.sighup_handler()
            self.assertRaises(SleepException, self.p.run)
        with patch('patroni.config.Config.set_dynamic_configuration', Mock(return_value=True)):
            self.assertRaises(SleepException, self.p.run)
        with patch('patroni.postgresql.Postgresql.data_directory_empty', Mock(return_value=False)):
            self.assertRaises(SleepException, self.p.run)

    def test_sigterm_handler(self):
        self.assertRaises(SystemExit, self.p.sigterm_handler)

    def test_schedule_next_run(self):
        self.p.ha.cluster = Mock()
        self.p.ha.dcs.watch = Mock(return_value=True)
        self.p.schedule_next_run()
        self.p.next_run = time.time() - self.p.dcs.loop_wait - 1
        self.p.schedule_next_run()

    def test_noloadbalance(self):
        self.p.tags['noloadbalance'] = True
        self.assertTrue(self.p.noloadbalance)

    def test_nofailover(self):
        self.p.tags['nofailover'] = True
        self.assertTrue(self.p.nofailover)
        self.p.tags['nofailover'] = None
        self.assertFalse(self.p.nofailover)

    def test_replicatefrom(self):
        self.assertIsNone(self.p.replicatefrom)
        self.p.tags['replicatefrom'] = 'foo'
        self.assertEqual(self.p.replicatefrom, 'foo')

    def test_reload_config(self):
        self.p.reload_config()
        self.p.get_tags = Mock(side_effect=Exception)
        self.p.reload_config(local=True)

    def test_nosync(self):
        self.p.tags['nosync'] = True
        self.assertTrue(self.p.nosync)
        self.p.tags['nosync'] = None
        self.assertFalse(self.p.nosync)

    @patch.object(Thread, 'join', Mock())
    def test_shutdown(self):
        self.p.api.shutdown = Mock(side_effect=Exception)
        self.p.ha.shutdown = Mock(side_effect=Exception)
        self.p.shutdown()

    def test_check_psycopg(self):
        with patch('builtins.__import__', Mock(side_effect=ImportError)):
            self.assertRaises(SystemExit, check_psycopg)
        with patch('builtins.__import__', mock_import):
            self.assertRaises(SystemExit, check_psycopg)
