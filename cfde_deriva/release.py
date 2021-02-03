
import os
import sys
import logging

from deriva.core import DerivaServer, get_credential

from .tableschema import ReleaseConfigurator
from .datapackage import CfdeDataPackage, portal_schema_json
from .registry import Registry

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))

def main(subcommand, *args):
    """Ugly test-harness for data release.

    Usage: python3 -m cfde_deriva.release <sub-command> ...

    Sub-commands:
    - 'provision'
       - Create new, empty release catalog
    - 'load' catalog_id filename...
       - Load one or more datapackage files into existing catalog
    - 'reconfigure' catalog_id
       - Revise policy/resentation config on existing catalog

    This client uses default DERIVA credentials for server.

    Set environment variable DERIVA_SERVERNAME to choose service host.

    """
    logger.addHandler(logging.StreamHandler(stream=sys.stderr))

    servername = os.getenv('DERIVA_SERVERNAME', 'app-dev.nih-cfde.org')
    credential = get_credential(servername)
    registry = Registry('https', servername, credentials=credential)
    server = DerivaServer('https', servername, credential)

    if subcommand == 'provision':
        catalog = server.create_ermrest_catalog()
        print('New catalog has catalog_id=%s' % catalog.catalog_id)
        dp = CfdeDataPackage(portal_schema_json, ReleaseConfigurator())
        dp.set_catalog(catalog, registry)
        dp.provision()
        print("Model deployed for %s." % (dp.package_filename,))
        dp.apply_custom_config()
        print('Policies and presentation configured for %s.' % (catalog._server_uri,))
    elif subcommand == 'load':
        if len(args) > 1:
            catalog_id = args[0]
        else:
            raise TypeError('"load" requires two or more positional argument: catalog_id filename...')

        for filename in args[1:]:
            dp = CfdeDataPackage(filename, ReleaseConfigurator())
            dp.provision()
            dp.set_catalog(catalog, registry)
            dp.load_data_files(onconflict='skip')

        print("All data packages loaded.")
    elif subcommand == 'reconfigure':
        if len(args) == 1:
            catalog_id = args[0]
        else:
            raise TypeError('"reconfigure" requires exactly one positional argument: catalog_id')

        catalog = server.connect_ermrest(catalog_id)
        dp = CfdeDataPackage(portal_schema_json, ReleaseConfigurator())
        dp.set_catalog(catalog, registry)
        dp.apply_custom_config()
        print('Policies and presentation configured for %s.' % (catalog._server_uri,))
    else:
        raise ValueError('unknown sub-command "%s"' % subcommand)

if __name__ == '__main__':
    exit(main(*sys.argv[1:]))

