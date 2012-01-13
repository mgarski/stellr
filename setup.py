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