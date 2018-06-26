from datalabframework import params

import os
from textwrap import dedent

import pytest
from testfixtures import TempDirectory


@pytest.fixture()
def dir():
    with TempDirectory() as dir:
        os.chdir(dir.path)
        yield dir

class Test_rootpath(object):
    def test_minimal(self, dir):
        yml = b'''\
               ---
               a:
                 b: 'ohoh'
                 c: 42
                 s: 1
               '''
        dir.write('metadata.yml', dedent(yml))
        assert(params.metadata()=={'a': {'b': 'ohoh', 'c': 42, 's': 1}, 'resources': {}})

    def test_minimal_with_resources(self, dir):
        yml = b'''\
                ---
                a:
                    b: 'ohoh'
                    c: 42
                    s: 1
                resources:
                    hello:
                        best:resource
               '''
        dir.write('metadata.yml', dedent(yml))
        assert(params.metadata()=={'a': {'b': 'ohoh', 'c': 42, 's': 1}, 'resources': { '.hello': 'best:resource'}})

    def test_multiple_docs(self,dir):
        yml = b'''\
                ---
                a:
                    b: 'ohoh'
                resources:
                    hello:
                        a:1
                ---
                run: second
                c:
                    d: 'lalala'
                resources:
                    world:
                        b: 2
               '''
        dir.write('metadata.yml', dedent(yml))
        assert(params.metadata()=={'a': {'b': 'ohoh'}, 'resources': {'.hello': 'a:1'}})
        assert(params.metadata(True)=={
            'default': {'a': {'b': 'ohoh'}, 'resources': {'.hello': 'a:1'}},
            'second': {'c': {'d': 'lalala'},'resources': {'.world': {'b': 2}},'run': 'second'}
            })

    def test_multiple_files(self,dir):
        yml_1 = b'''\
                ---
                a:
                    b: 'ohoh'
                ---
                run: second
                c:
                    d: 'lalala'
               '''
        yml_2 = b'''\
                ---
                resources:
                    hello:
                        a:1
                ---
                run: second
                resources:
                    world:
                        b: 2
               '''

        subdir = dir.makedir('abc')
        dir.write('metadata.yml', dedent(yml_1))
        dir.write('abc/metadata.yml', dedent(yml_2))
        assert(params.metadata()=={'a': {'b': 'ohoh'}, 'resources': {'.abc.hello': 'a:1'}})
        assert(params.metadata(True)=={
            'default': {'a': {'b': 'ohoh'}, 'resources': {'.abc.hello': 'a:1'}},
            'second': {'c': {'d': 'lalala'},'resources': {'.abc.world': {'b': 2}},'run': 'second'}
            })
