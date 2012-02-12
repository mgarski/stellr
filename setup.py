#   Copyright 2011-2012 Michael Garski (mgarski@mac.com)
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from setuptools import setup

setup(
    name='stellr',
    version='0.3.2',
    description='Solr client library for gevent utilizing urllib3 and ZeroMQ.',
    author='Michael Garski',
    author_email='mgarski@mac.com',
    #TODO: is 'depends' valid? seeing warnings that it is not
    install_requires=['urllib3>=1.1',
             'gevent>=0.13.6',
             'gevent_zeromq>=0.2.2',
             'pyzmq>=2.0.10.1',
             'simplejson>=2.1.6'],
    url='https://github.com/mgarski/stellr',
    packages=['stellr']
)