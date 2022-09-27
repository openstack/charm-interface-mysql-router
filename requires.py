from charmhelpers.core import unitdata
from charms.reactive import (
    Endpoint,
    when,
    set_flag,
    clear_flag,
)


# NOTE: fork of relations.AutoAccessors for forwards compat behaviour
class MySQLRouterAutoAccessors(type):
    """
    Metaclass that converts fields referenced by ``auto_accessors`` into
    accessor methods with very basic doc strings.
    """
    def __new__(cls, name, parents, dct):
        for field in dct.get('auto_accessors', []):
            meth_name = field.replace('-', '_')
            meth = cls._accessor(field)
            meth.__name__ = meth_name
            meth.__module__ = dct.get('__module__')
            meth.__doc__ = 'Get the %s, if available, or None.' % field
            dct[meth_name] = meth
        return super(MySQLRouterAutoAccessors, cls).__new__(
            cls, name, parents, dct
        )

    @staticmethod
    def _accessor(field):
        def __accessor(self):
            return self.all_joined_units.received_raw.get(field)
        return __accessor


class MySQLRouterRequires(Endpoint, metaclass=MySQLRouterAutoAccessors):

    key = 'reactive.conversations.db-router.global.local-data.{}'

    kv = unitdata.kv()

    auto_accessors = ['db_host', 'ssl_ca', 'ssl_cert', 'ssl_key',
                      'wait_timeout']

    @when('endpoint.{endpoint_name}.joined')
    def joined(self):
        set_flag(self.expand_name('{endpoint_name}.connected'))
        self.set_or_clear_available()

    @when('endpoint.{endpoint_name}.changed')
    def changed(self):
        self.joined()

    def set_or_clear_available(self):
        if self.db_router_data_complete():
            set_flag(self.expand_name('{endpoint_name}.available'))
        else:
            clear_flag(self.expand_name('{endpoint_name}.available'))
        if self.proxy_db_data_complete():
            set_flag(self.expand_name('{endpoint_name}.available.proxy'))
        else:
            clear_flag(self.expand_name('{endpoint_name}.available.proxy'))
        if self.ssl_data_complete():
            set_flag(self.expand_name('{endpoint_name}.available.ssl'))
        else:
            clear_flag(self.expand_name('{endpoint_name}.available.ssl'))

    @when('endpoint.{endpoint_name}.broken')
    def broken(self):
        self.departed()

    @when('endpoint.{endpoint_name}.departed')
    def departed(self):
        # Clear state
        clear_flag(self.expand_name('{endpoint_name}.connected'))
        clear_flag(self.expand_name('{endpoint_name}.available'))
        clear_flag(self.expand_name('{endpoint_name}.proxy.available'))
        clear_flag(self.expand_name('{endpoint_name}.available.ssl'))
        # Check if this is the last unit
        last_unit = True
        for relation in self.relations:
            if len(relation.units) > 0:
                # This is not the last unit so reevaluate state
                self.joined()
                self.changed()
                last_unit = False
        if last_unit:
            # Bug #1972883
            self._set_local('prefixes', [])

    def configure_db_router(self, username, hostname, prefix):
        """
        Called by charm layer that uses this interface to configure a database.
        """

        relation_info = {
            prefix + '_username': username,
            prefix + '_hostname': hostname,
            'private-address': hostname,
        }
        self.set_prefix(prefix)
        for relation in self.relations:
            for k, v in relation_info.items():
                relation.to_publish_raw[k] = v
                self._set_local(k, v)

    def configure_proxy_db(self, database, username, hostname, prefix):
        """
        Called by charm layer that uses this interface to configure a database.
        """

        relation_info = {
            prefix + '_database': database,
            prefix + '_username': username,
            prefix + '_hostname': hostname,
        }
        self.set_prefix(prefix)
        for relation in self.relations:
            for k, v in relation_info.items():
                relation.to_publish_raw[k] = v
                self._set_local(k, v)

    def _set_local(self, key, value):
        self.kv.set(self.key.format(key), value)

    def _get_local(self, key):
        return self.kv.get(self.key.format(key))

    def set_prefix(self, prefix):
        """
        Store all of the database prefixes in a list.
        """
        prefixes = self._get_local('prefixes')
        for relation in self.relations:
            if prefixes:
                if prefix not in prefixes:
                    self._set_local('prefixes', prefixes + [prefix])
            else:
                self._set_local('prefixes', [prefix])

    def get_prefixes(self):
        """
        Return the list of saved prefixes.
        """
        return self._get_local('prefixes')

    def database(self, prefix):
        """
        Return a configured database name.
        """
        return self._get_local(prefix + '_database')

    def username(self, prefix):
        """
        Return a configured username.
        """
        return self._get_local(prefix + '_username')

    def hostname(self, prefix):
        """
        Return a configured hostname.
        """
        return self._get_local(prefix + '_hostname')

    def _received_app(self, key):
        value = None
        for relation in self.relations:
            value = relation.received_app_raw.get(key)
            if value:
                return value
        # NOTE(ganso): backwards compatibility with non-app-bag below
        if not value:
            return self.all_joined_units.received_raw.get(key)

    def password(self, prefix):
        """
        Return a database password.
        """
        return self._received_app(prefix + '_password')

    def allowed_units(self, prefix):
        """
        Return a database's allowed_units.
        """
        return self._received_app(prefix + '_allowed_units')

    def _read_suffixes(self, suffixes):
        data = {}
        for prefix in self.get_prefixes():
            for suffix in suffixes:
                key = prefix + suffix
                data[key] = self._received_app(key)
        return data

    def db_router_data_complete(self):
        """
        Check if required db router data is complete.
        """
        data = {
            'db_host': self.db_host(),
        }
        if self.get_prefixes():
            suffixes = ['_password']
            data.update(self._read_suffixes(suffixes))
            if all(data.values()):
                return True
        return False

    def proxy_db_data_complete(self):
        """
        Check if required proxy databases data is complete.
        """
        data = {
            'db_host': self.db_host(),
        }
        # The mysql-router prefix + proxied db prefixes
        if self.get_prefixes() and len(self.get_prefixes()) > 1:
            suffixes = ['_password', '_allowed_units']
            data.update(self._read_suffixes(suffixes))
            if all(data.values()):
                return True
        return False

    def ssl_data_complete(self):
        """
        Check if optional ssl data provided by mysql is complete.
        """
        data = {
            'ssl_ca': self.ssl_ca(),
        }
        if all(data.values()):
            return True
        return False
