# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import charms_openstack.test_utils as test_utils
from unittest import mock

import requires


_hook_args = {}


def mock_hook(*args, **kwargs):

    def inner(f):
        # remember what we were passed.  Note that we can't actually determine
        # the class we're attached to, as the decorator only gets the function.
        _hook_args[f.__name__] = dict(args=args, kwargs=kwargs)
        return f
    return inner


class TestMySQLRouterRequires(test_utils.PatchHelper):

    @classmethod
    def setUpClass(cls):
        cls._patched_hook = mock.patch('charms.reactive.hook', mock_hook)
        cls._patched_hook_started = cls._patched_hook.start()
        # force requires to rerun the mock_hook decorator:
        # try except is Python2/Python3 compatibility as Python3 has moved
        # reload to importlib.
        try:
            reload(requires)
        except NameError:
            import importlib
            importlib.reload(requires)

    @classmethod
    def tearDownClass(cls):
        cls._patched_hook.stop()
        cls._patched_hook_started = None
        cls._patched_hook = None
        # and fix any breakage we did to the module
        try:
            reload(requires)
        except NameError:
            import importlib
            importlib.reload(requires)

    def setUp(self):
        self._patches = {}
        self._patches_start = {}

        self._rel_ids = ["db-router:3"]
        self._remote_data = {}
        self._published_data = {}
        self._local_data = {}

        # The Relation object
        self.fake_relation = mock.MagicMock()
        self.fake_unit = mock.MagicMock()
        self.fake_unit.unit_name = "unit/1"
        self.fake_relation.relation_id = self._rel_ids[0]
        self.fake_relation.units = [self.fake_unit]
        self.mysql_router = requires.MySQLRouterRequires(
            'mysql-router', self._rel_ids)
        self.mysql_router._get_local = mock.MagicMock(
            side_effect=self.get_fake_local_data)
        self.mysql_router.relations[0] = self.fake_relation
        self.fake_relation.to_publish_raw = self._published_data
        self.fake_relation.received_app_raw = self._remote_data
        self.patch_mysql_router('_set_local')
        self.patch_object(requires, "clear_flag")
        self.patch_object(requires, "set_flag")
        self.patch_mysql_router('db_host', "10.5.0.21")
        self.patch_mysql_router('wait_timeout', 90)

    def tearDown(self):
        self.mysql_router = None
        for k, v in self._patches.items():
            v.stop()
            setattr(self, k, None)
        self._patches = None
        self._patches_start = None

    def patch_mysql_router(self, attr, return_value=None):
        mocked = mock.patch.object(self.mysql_router, attr)
        self._patches[attr] = mocked
        started = mocked.start()
        started.return_value = return_value
        self._patches_start[attr] = started
        setattr(self, attr, started)

    def get_fake_local_data(self, key, default=None):
        return self._local_data.get(key) or default

    def test_registered_hooks(self):
        # test that the hooks actually registered the relation expressions that
        # are meaningful for this interface: this is to handle regressions.
        # The keys are the function names that the hook attaches to.
        hook_patterns = {
            'joined': ('{requires:mysql-router}-relation-joined',),
            'changed': ('{requires:mysql-router}-relation-changed',),
            'departed': (
                '{requires:mysql-router}-relation-{broken,departed}',)}
        for k, v in _hook_args.items():
            self.assertEqual(hook_patterns[k], v['args'])

    def test_changed_available(self):
        self.patch_mysql_router('db_router_data_complete', True)
        self.patch_mysql_router('proxy_db_data_complete', True)
        self.patch_mysql_router('ssl_data_complete', True)
        self._local_data = {"prefixes": ["myprefix"]}
        _calls = [
            mock.call("mysql-router.available"),
            mock.call("mysql-router.available.proxy"),
            mock.call("mysql-router.available.ssl")]
        self.mysql_router.set_or_clear_available()
        self.set_flag.assert_has_calls(_calls)

    def test_changed_not_available(self):
        self.patch_mysql_router('db_router_data_complete', False)
        self.patch_mysql_router('joined')
        self._local_data = {"prefixes": ["myprefix"]}
        self.mysql_router.set_or_clear_available()
        self.set_flag.assert_not_called()

    def test_joined(self):
        self.patch_mysql_router('set_or_clear_available')
        self.mysql_router.joined()
        self.set_flag.assert_called_once_with('mysql-router.connected')
        self.set_or_clear_available.assert_called_once()

    def test_departed(self):
        self.mysql_router.departed()
        _calls = [
            mock.call('mysql-router.connected'),
            mock.call("mysql-router.available"),
            mock.call('mysql-router.proxy.available'),
            mock.call('mysql-router.available.ssl')]
        self.clear_flag.assert_has_calls(_calls)

    def test_db_router_data_complete_missing_prefix(self):
        self._remote_data.update({"password": "1234",
                                  "allowed_units": "unit/1"})
        assert self.mysql_router.db_router_data_complete() is False

    def test_db_router_data_complete(self):
        self._local_data = {"prefixes": ["myprefix"]}
        self._remote_data.update({"myprefix_password": "1234",
                                  "myprefix_allowed_units": "unit/1"})
        assert self.mysql_router.db_router_data_complete() is True
        self.db_host.return_value = None
        assert self.mysql_router.db_router_data_complete() is False

    def test_db_router_data_complete_wait_timeout(self):
        self._local_data = {"prefixes": ["myprefix"]}
        self._remote_data.update({"myprefix_password": "1234",
                                  "myprefix_allowed_units": "unit/1"})
        # Wait timeout is an optional value and should not affect data complete
        self.wait_timeout.return_value = None
        assert self.mysql_router.db_router_data_complete() is True
        self.wait_timeout.return_value = 90
        assert self.mysql_router.db_router_data_complete() is True

    def test_proxy_db_data_incomplete(self):
        self._local_data = {"prefixes": ["myprefix"]}
        self._remote_data.update({"myprefix_password": "1234",
                                  "myprefix_allowed_units": "unit/1"})
        assert self.mysql_router.proxy_db_data_complete() is False

    def test_proxy_db_data_complete(self):
        self._local_data = {"prefixes": ["myprefix", "db"]}
        self._remote_data.update({"myprefix_password": "1234",
                                  "myprefix_allowed_units": "unit/1",
                                  "db_password": "1234",
                                  "db_allowed_units": "unit/1"})
        assert self.mysql_router.proxy_db_data_complete() is True
        self.db_host.return_value = None
        assert self.mysql_router.proxy_db_data_complete() is False

    def test_ssl_data_incomplete(self):
        self.patch_mysql_router('ssl_ca', "Certificate Authority")
        self.patch_mysql_router('ssl_cert', "somecert")
        self.patch_mysql_router('ssl_key', "somekey")
        assert self.mysql_router.ssl_data_complete() is True
        self.ssl_ca.return_value = None
        assert self.mysql_router.ssl_data_complete() is False

    def test_local_accessors(self):
        _prefix = "myprefix"
        _value = "value"
        _tests = {
            "database": self.mysql_router.database,
            "username": self.mysql_router.username,
            "hostname": self.mysql_router.hostname}
        # Not set
        for key, test in _tests.items():
            self.assertEqual(test(_prefix), None)
        # Set
        self._local_data = {"prefixes": [_prefix]}
        for key, test in _tests.items():
            self._local_data["{}_{}".format(_prefix, key)] = _value
            self.assertEqual(test(_prefix), _value)

    def test_remote_accessors(self):
        _prefix = "myprefix"
        _value = "value"
        _tests = {
            "password": self.mysql_router.password,
            "allowed_units": self.mysql_router.allowed_units}
        # Not set
        for key, test in _tests.items():
            self.assertEqual(test(_prefix), None)
        # Set
        for key, test in _tests.items():
            self._remote_data.update({"{}_{}".format(_prefix, key): _value})
            self.assertEqual(test(_prefix), _value)

    def test_configure_db_router(self):
        self.patch_mysql_router('set_prefix')
        _user = "user"
        _host = "host"
        _prefix = "prefix"
        _expected = {
            "{}_username".format(_prefix): _user,
            "{}_hostname".format(_prefix): _host,
            "private-address": _host}
        calls = [
            mock.call('prefix_username', _user),
            mock.call('prefix_hostname', _host),
            mock.call('private-address', _host),
        ]
        self.mysql_router.configure_db_router(_user, _host, prefix=_prefix)
        self._set_local.has_calls(calls)
        self.assertTrue(all(self._published_data[k] == _expected[k]
                            for k in _expected.keys()))
        self.set_prefix.assert_called_once()

    def test_configure_proxy_db(self):
        self.patch_mysql_router('set_prefix')
        _db = "db"
        _user = "user"
        _host = "host"
        _prefix = "prefix"
        _expected = {
            "{}_database".format(_prefix): _db,
            "{}_username".format(_prefix): _user,
            "{}_hostname".format(_prefix): _host}
        self.mysql_router.configure_proxy_db(_db, _user, _host, prefix=_prefix)
        calls = [
            mock.call('prefix_database', _db),
            mock.call('prefix_username', _user),
            mock.call('prefix_hostname', _host)
        ]
        self._set_local.has_calls(calls)

        self.assertTrue(all(self._published_data[k] == _expected[k]
                            for k in _expected.keys()))
        self.set_prefix.assert_called_once()

    def test_get_prefix(self):
        _prefix = "prefix"
        self._local_data = {"prefixes": [_prefix]}
        self.assertEqual(
            self.mysql_router.get_prefixes(), [_prefix])

    def test_set_prefix(self):
        # First
        _prefix = "prefix"
        self.mysql_router.set_prefix(_prefix)
        self._set_local.assert_called_once_with("prefixes", [_prefix])
        # More than one
        self._set_local.reset_mock()
        self._local_data = {"prefixes": [_prefix]}
        _second = "secondprefix"
        self.mysql_router.set_prefix(_second)
        self._set_local.assert_called_once_with("prefixes", [_prefix, _second])

    def test_ly_departed(self):
        self._local_data = {"prefixes": ["myprefix"]}
        self.patch_mysql_router('ssl_ca', "fake_ca")

        self.mysql_router.departed()
        self.assertFalse(self._set_local.called)

        self.mysql_router.relations[0].units = []
        self.mysql_router.departed()
        self._set_local.assert_called_once_with("prefixes", [])
