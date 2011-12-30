from distutils.core import setup

setup(
    name='stellr',
    version='0.2.0',
    description='Solr client library for Eventlet utilizing urllib3.',
    author='Michael Garski',
    author_email='mgarski@mac.com',
    depends='urllib3>1.0',
    url='https://github.com/mgarski/stellr',
    packages=['stellr']
)