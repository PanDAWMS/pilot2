import logging
import os

logger = logging.getLogger(__name__)
ssl_context = None


def capath(args=None):
    if args is not None and args['capath'] is not None and os.path.isdir(args['capath']):
        return args['capath']

    path = os.environ.get('X509_CERT_DIR', '/etc/grid-security/certificates')
    if os.path.isdir(path):
        return path

    return None


def cacert_default_location():
    try:
        return '/tmp/x509up_u%s' % str(os.getuid())
    except AttributeError:
        # Wow, not UNIX? Nevermind, skip.
        pass

    return None


def cacert(args=None):
    if args is not None and args['cacert'] is not None and os.path.isfile(args['cacert']):
        return args['cacert']

    path = os.environ.get('X509_USER_PROXY', cacert_default_location())
    if os.path.isfile(path):
        return path

    return None


def setup(args):
    global ssl_context

    try:
        import ssl
        ssl_context = ssl.create_default_context(
            capath=capath(args),
            cafile=cacert(args))
    except Exception as e:
        logger.warn('SSL communication is impossible due to SSL error:')
        logger.warn(e.message)
        pass
