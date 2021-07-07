import sys
import logging
from deriva.core import bootstrap, get_credential, GlobusNativeLogin

logger = logging.getLogger(__name__)

CFDE_DERIVA_SCOPE = "https://auth.globus.org/scopes/app.nih-cfde.org/deriva_all"

HOST_TO_GCS_SCOPES = {
    "app": "https://auth.globus.org/scopes/d4c89edc-a22c-4bc3-bfa2-bca5fd19b404/https",
    "app.nih-cfde.org": "https://auth.globus.org/scopes/d4c89edc-a22c-4bc3-bfa2-bca5fd19b404/https",
    "app-dev": "https://auth.globus.org/scopes/36530efa-a1e3-45dc-a6e7-9560a8e9ac49/https",
    "app-dev.nih-cfde.org": "https://auth.globus.org/scopes/36530efa-a1e3-45dc-a6e7-9560a8e9ac49/https",
    "app-staging": "https://auth.globus.org/scopes/922ee14d-49b7-4d69-8f1c-8e2ff8207542/https",
    "app-staging.nih-cfde.org": "https://auth.globus.org/scopes/922ee14d-49b7-4d69-8f1c-8e2ff8207542/https"
}

HOST_TO_GCS_ENDPOINTS = {
    "app": "https://g-882990.aa98d.08cc.data.globus.org",
    "app.nih-cfde.org": "https://g-882990.aa98d.08cc.data.globus.org",
    "app-dev": "https://g-c7e94.f19a4.5898.data.globus.org",
    "app-dev.nih-cfde.org": "https://g-c7e94.f19a4.5898.data.globus.org",
    "app-staging": "https://g-3368fe.c0aba.03c0.data.globus.org",
    "app-staging.nih-cfde.org": "https://g-3368fe.c0aba.03c0.data.globus.org"
}


def get_archive_headers_map(host):
    scope = HOST_TO_GCS_SCOPES[host]
    gnl = GlobusNativeLogin()
    headers = {}
    tokens = gnl.is_logged_in(requested_scopes=(scope, CFDE_DERIVA_SCOPE))
    if tokens:
        headers.update({'Authorization': 'Bearer %s' % gnl.find_access_token_for_scope(scope, tokens)})
    else:
        raise ValueError('Could not obtain tokens for host "%s"' % host)
    return {
        '.*': {'X-Requested-With': 'XMLHttpRequest'},
        "%s/.*" % HOST_TO_GCS_ENDPOINTS[host]: headers,
    }


def main(subcommand, *args):
    bootstrap()
    if subcommand == 'login':
        if len(args) > 0 and args[0] in HOST_TO_GCS_SCOPES.keys():
            host = args[0]
            scope = HOST_TO_GCS_SCOPES[host]
            gnl = GlobusNativeLogin()
            tokens = gnl.login(no_browser=True, no_local_server=True, requested_scopes=(scope, CFDE_DERIVA_SCOPE))
            access_token = gnl.find_access_token_for_scope(scope, tokens)
            print('Logged into host "%s" with scope: %s' % (host, scope))
        else:
            raise ValueError("Expected hostname, one of the following: %s" % list(HOST_TO_GCS_SCOPES.keys()))
    elif subcommand == 'logout':
        if len(args) > 0 and args[0] in HOST_TO_GCS_SCOPES.keys():
            host = args[0]
            scope = HOST_TO_GCS_SCOPES[host]
            gnl = GlobusNativeLogin()
            gnl.logout(requested_scopes=(scope, CFDE_DERIVA_SCOPE))
            print('Logged out of host "%s" with scope: %s' % (host, scope))
        else:
            raise ValueError("Expected hostname, one of the following: %s" % list(HOST_TO_GCS_SCOPES.keys()))
    elif subcommand == 'headers':
        if len(args) > 0 and args[0] in HOST_TO_GCS_SCOPES.keys() and args[0] in HOST_TO_GCS_ENDPOINTS.keys():
            host = args[0]
            url = HOST_TO_GCS_ENDPOINTS[host]
            headers = get_archive_headers_map(host)
            if headers:
                print('Header map for "%s" (%s):\n%s' % (host, url, headers))
            else:
                print('Login required for host: "%s"' % host)
        else:
            raise ValueError("Expected hostname, one of the following: %s" % list(HOST_TO_GCS_SCOPES.keys()))
    elif subcommand == 'credential':
        if len(args) > 0 and args[0] in HOST_TO_GCS_SCOPES.keys() and args[0] in HOST_TO_GCS_ENDPOINTS.keys():
            host = args[0]
            url = HOST_TO_GCS_ENDPOINTS[host]
            credential = get_credential(host)
            if credential:
                print('Credential for "%s" (%s):\n%s' % (host, url, credential))
            else:
                print('Login required for host: "%s"' % host)
        else:
            raise ValueError("Expected hostname, one of the following: %s" % list(HOST_TO_GCS_SCOPES.keys()))
    else:
        raise ValueError('unknown sub-command "%s"' % subcommand)


if __name__ == '__main__':
    exit(main(*sys.argv[1:]))
