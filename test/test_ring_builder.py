# Copyright (c) 2010-2012 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from webob import Request
import os
import shutil
import swift.common.ring.utils
import swift.common.utils
from swift.common.exceptions import LockTimeout, RingFileChanged
import cPickle as pickle
import gzip
from swift.common.ring import RingBuilder
from mock import MagicMock, call as mock_call
import json
import errno


class FakeApp(object):
    def __call__(self, env, start_response):
        return 'FakeApp'


class FakedBuilder(object):

    def __init__(self, device_count=5):
        self.device_count = device_count

    def create_builder(self):
        builder = RingBuilder(18, 3, 1)
        for i in xrange(self.device_count):
            zone = i
            ipaddr = "1.1.1.1"
            port = 6010
            device_name = "sd%s" % i
            weight = 1.0
            meta = "meta for %s" % i
            next_dev_id = 0
            if builder.devs:
                next_dev_id = max(d['id'] for d in builder.devs if d) + 1
            builder.add_dev({'id': next_dev_id, 'zone': zone, 'ip': ipaddr,
                             'port': int(port), 'device': device_name,
                             'weight': weight, 'meta': meta})
        return builder


class TestRingBuilder(unittest.TestCase):

    def setUp(self):
        self.real_search_devs = swift.common.ring.utils.search_devs
        self.search_result = {'id': 1, 'weight': 5}
        swift.common.ring.utils.search_devs = \
            MagicMock(return_value=self.search_result)
        self.real_lock_file = swift.common.utils.lock_file
        swift.common.utils.lock_file = MagicMock()
        self.real_pickle_dump = pickle.dump
        pickle.dump = MagicMock(return_value=True)
        self.real_gzip = gzip.GzipFile
        gzip.GzipFile = MagicMock()
        self.real_mkdir = os.mkdir
        os.mkdir = MagicMock()
        from rbm import ring_builder
        tb = FakedBuilder()
        self.mock_builder = tb.create_builder()
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(),
                                                      {'key': 'something'})
        self.app._get_md5sum = MagicMock(return_value="newhash")
        self.app._make_backup = MagicMock(return_value=True)
        self.app.write_builder = MagicMock(return_value="newhash")

    def tearDown(self):
        pickle.dump = self.real_pickle_dump
        gzip.Gzip = self.real_gzip
        swift.common.utils.lock_file = self.real_lock_file
        swift.common.ring.utils.search_devs = self.real_search_devs
        os.mkdir = self.real_mkdir

    def test_head_builder(self):
        start_response = MagicMock(return_value="MOCKED")

        req = Request.blank('/ringbuilder/object.builder',
                            environ={'REQUEST_METHOD': 'HEAD',
                                     'HTTP_X_RING_BUILDER_KEY': 'something'})
        resp = self.app(req.environ, start_response)
        ob = '/etc/swift/object.builder'
        self.app._get_md5sum.assert_called_once_with(ob)
        start_response.assert_called_once_with('200 OK',
                                               [('X-Current-Hash', 'newhash')])
        self.app._get_md5sum.reset_mock()
        start_response.reset_mock()
        req = Request.blank('/ringbuilder/container.builder',
                            environ={'REQUEST_METHOD': 'HEAD',
                                     'HTTP_X_RING_BUILDER_KEY': 'something'})
        resp = self.app(req.environ, start_response)
        cb = '/etc/swift/container.builder'
        self.app._get_md5sum.assert_called_once_with(cb)
        start_response.assert_called_once_with('200 OK',
                                               [('X-Current-Hash', 'newhash')])
        self.app._get_md5sum.reset_mock()
        start_response.reset_mock()
        req = Request.blank('/ringbuilder/account.builder',
                            environ={'REQUEST_METHOD': 'HEAD',
                                     'HTTP_X_RING_BUILDER_KEY': 'something'})
        resp = self.app(req.environ, start_response)
        ab = '/etc/swift/account.builder'
        self.app._get_md5sum.assert_called_once_with(ab)
        start_response.assert_called_once_with('200 OK',
                                               [('X-Current-Hash', 'newhash')])

    def test_head_ring(self):
        start_response = MagicMock(return_value="MOCKED")
        req = Request.blank('/ring/object.ring.gz',
                            environ={'REQUEST_METHOD': 'HEAD',
                                     'HTTP_X_RING_BUILDER_KEY': 'something'})
        resp = self.app(req.environ, start_response)
        objr = '/etc/swift/object.ring.gz'
        self.app._get_md5sum.assert_called_once_with(objr)
        start_response.assert_called_once_with('200 OK',
                                               [('X-Current-Hash', 'newhash')])
        self.app._get_md5sum.reset_mock()
        start_response.reset_mock()
        req = Request.blank('/ring/container.ring.gz',
                            environ={'REQUEST_METHOD': 'HEAD',
                                     'HTTP_X_RING_BUILDER_KEY': 'something'})
        resp = self.app(req.environ, start_response)
        cr = '/etc/swift/container.ring.gz'
        self.app._get_md5sum.assert_called_once_with(cr)
        start_response.assert_called_once_with('200 OK',
                                               [('X-Current-Hash', 'newhash')])
        self.app._get_md5sum.reset_mock()
        start_response.reset_mock()
        req = Request.blank('/ring/account.ring.gz',
                            environ={'REQUEST_METHOD': 'HEAD',
                                     'HTTP_X_RING_BUILDER_KEY': 'something'})
        resp = self.app(req.environ, start_response)
        ar = '/etc/swift/account.ring.gz'
        self.app._get_md5sum.assert_called_once_with(ar)
        start_response.assert_called_once_with('200 OK',
                                               [('X-Current-Hash', 'newhash')])

    def test_bad_method(self):
        start_response = MagicMock(return_value="MOCKED")
        req = Request.blank('/ring/object',
                            environ={'REQUEST_METHOD': 'INVALID',
                                     'HTTP_X_RING_BUILDER_KEY': 'something'})
        resp = self.app(req.environ, start_response)
        objr = '/etc/swift/object.ring.gz'
        #self.app._get_md5sum.assert_called_once_with(objr)
        start_response.assert_called_once_with('400 Bad Request',
                                               [('Content-Length', '16'),
                                                ('Content-Type',
                                                 'text/plain')])
        start_response.reset_mock()
        req = Request.blank('/ringbuilder/object',
                            environ={'REQUEST_METHOD': 'INVALID',
                                     'HTTP_X_RING_BUILDER_KEY': 'something'})
        resp = self.app(req.environ, start_response)
        objr = '/etc/swift/object.ring.gz'
        #self.app._get_md5sum.assert_called_once_with(objr)
        start_response.assert_called_once_with('400 Bad Request',
                                               [('Content-Length', '16'),
                                                ('Content-Type',
                                                 'text/plain')])

    def test_no_ring_key(self):
        start_response = MagicMock(return_value="MOCKED")
        req = Request.blank('/ring/object',
                            environ={'REQUEST_METHOD': 'INVALID'})
        resp = self.app(req.environ, start_response)
        objr = '/etc/swift/object.ring.gz'
        #self.app._get_md5sum.assert_called_once_with(objr)
        start_response.assert_called_once_with('401 Unauthorized',
                                               [('Content-Length', '0')])
        start_response.reset_mock()
        req = Request.blank('/ringbuilder/object',
                            environ={'REQUEST_METHOD': 'INVALID'})
        resp = self.app(req.environ, start_response)
        objr = '/etc/swift/object.ring.gz'
        #self.app._get_md5sum.assert_called_once_with(objr)
        start_response.assert_called_once_with('401 Unauthorized',
                                               [('Content-Length', '0')])

    def test_unauthorized(self):
        start_response = MagicMock(return_value="MOCKED")
        req = Request.blank('/ring/object',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_X_RING_BUILDER_KEY': 'nope'})
        resp = self.app(req.environ, start_response)
        objr = '/etc/swift/object.ring.gz'
        #self.app._get_md5sum.assert_called_once_with(objr)
        start_response.assert_called_once_with('401 Unauthorized',
                                               [('Content-Length', '0')])
        start_response.reset_mock()
        req = Request.blank('/ringbuilder/object',
                            environ={'REQUEST_METHOD': 'POST',
                                     'HTTP_X_RING_BUILDER_KEY': 'nope'})
        resp = self.app(req.environ, start_response)
        objr = '/etc/swift/object.ring.gz'
        #self.app._get_md5sum.assert_called_once_with(objr)
        start_response.assert_called_once_with('401 Unauthorized',
                                               [('Content-Length', '0')])

    def test_rb_search_account(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/account/search', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'value': 'd1'})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK',
                                               [('Content-Length', '22'),
                                                ('X-Current-Hash', 'newhash'),
                                                ('Content-Type',
                                                 'application/json')])
        bf = '/etc/swift/account.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        self.assertEquals(resp, ['{"id": 1, "weight": 5}'])

    def test_rb_search_container(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/container/search', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'value': 'd1'})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK',
                                               [('Content-Length', '22'),
                                                ('X-Current-Hash', 'newhash'),
                                                ('Content-Type',
                                                 'application/json')])
        bf = '/etc/swift/container.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        self.assertEquals(resp, ['{"id": 1, "weight": 5}'])

    def test_rb_search_object(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/object/search', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'value': 'd1'})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK',
                                               [('Content-Length', '22'),
                                                ('X-Current-Hash', 'newhash'),
                                                ('Content-Type',
                                                 'application/json')])
        bf = '/etc/swift/object.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        self.assertEquals(resp, ['{"id": 1, "weight": 5}'])

    def test_rb_meta_account(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/account/meta', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'devices': {'1': 'some metainfo'}})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/account.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        for dev in self.mock_builder.devs:
            if dev['id'] == 1:
                self.assertEquals(dev['meta'], 'some metainfo')

    def test_rb_meta_container(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/container/meta', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'devices': {'1': 'some metainfo'}})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/container.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        for dev in self.mock_builder.devs:
            if dev['id'] == 1:
                self.assertEquals(dev['meta'], 'some metainfo')

    def test_rb_meta_object(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/object/meta', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'devices': {'1': 'some metainfo'}})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/object.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        for dev in self.mock_builder.devs:
            if dev['id'] == 1:
                self.assertEquals(dev['meta'], 'some metainfo')

    def test_rb_weight_account(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/account/weight', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'devices': {'1': '5.0'}})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/account.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        for dev in self.mock_builder.devs:
            if dev['id'] == 1:
                self.assertEquals(dev['weight'], 5.0)

    def test_rb_weight_container(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/container/weight', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'devices': {'1': '5.0'}})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/container.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        for dev in self.mock_builder.devs:
            if dev['id'] == 1:
                self.assertEquals(dev['weight'], 5.0)

    def test_rb_weight_object(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/object/weight', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'devices': {'1': '5.0'}})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/object.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        for dev in self.mock_builder.devs:
            if dev['id'] == 1:
                self.assertEquals(dev['weight'], 5.0)

    def test_rb_remove_account(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/account/remove', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'devices': ["0"]})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/account.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        for dev in self.mock_builder.devs:
            if dev['id'] == 0:
                self.assertTrue(dev['weight'] == 0)

    def test_rb_remove_container(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/container/remove', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'devices': ["0"]})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/container.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        for dev in self.mock_builder.devs:
            if dev['id'] == 0:
                self.assertTrue(dev['weight'] == 0)

    def test_rb_remove_object(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/object/remove', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        req.body = json.dumps({'devices': ["0"]})
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/object.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        for dev in self.mock_builder.devs:
            if dev['id'] == 0:
                self.assertTrue(dev['weight'] == 0)

    def test_rb_add_account(self):

        def _dev_in_builder(builder, field='meta', match='test1'):
            for dev in builder.devs:
                if dev[field] == match:
                    return True
            return False

        def _get_dev_id(builder, field='meta', match='test1'):
            for dev in builder.devs:
                if dev[field] == match:
                    return dev['id']
            return False

        def _get_dev(builder, id):
            for dev in builder.devs:
                if dev['id'] == id:
                    return dev

        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/account/add', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        devices2add = {'devices': [{"weight": 15.0, "zone": 1,
                                    "ip": "1.1.1.1", "meta": "test1",
                                    "device": "sda", "port": 6010},
                                   {"weight": 15.0, "zone": 1,
                                    "ip": "1.1.1.1", "meta": "test2",
                                    "device": "sdb", "port": 6010}]}
        req.body = json.dumps(devices2add)
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/account.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        self.assertTrue(len(self.mock_builder.devs) == 7)
        self.assertTrue(_dev_in_builder(self.mock_builder, field='meta',
                                        match='test1'))
        id = _get_dev_id(self.mock_builder, field='meta', match='test1')
        device = _get_dev(self.mock_builder, id)
        for k in devices2add['devices'][0]:
            self.assertTrue(device[k] == devices2add['devices'][0][k])
        self.assertTrue(_dev_in_builder(self.mock_builder, field='meta',
                                        match='test2'))
        id = _get_dev_id(self.mock_builder, field='meta', match='test2')
        device = _get_dev(self.mock_builder, id)
        for k in devices2add['devices'][1]:
            self.assertTrue(device[k] == devices2add['devices'][1][k])

    def test_rb_add_container(self):

        def _dev_in_builder(builder, field='meta', match='test1'):
            for dev in builder.devs:
                if dev[field] == match:
                    return True
            return False

        def _get_dev_id(builder, field='meta', match='test1'):
            for dev in builder.devs:
                if dev[field] == match:
                    return dev['id']
            return False

        def _get_dev(builder, id):
            for dev in builder.devs:
                if dev['id'] == id:
                    return dev

        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)
        req = Request.blank('/ringbuilder/container/add', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        devices2add = {'devices': [{"weight": 15.0, "zone": 1,
                                    "ip": "1.1.1.1", "meta": "test1",
                                    "device": "sda", "port": 6010},
                                   {"weight": 15.0, "zone": 1,
                                    "ip": "1.1.1.1", "meta": "test2",
                                    "device": "sdb", "port": 6010}]}
        req.body = json.dumps(devices2add)
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/container.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        self.assertTrue(len(self.mock_builder.devs) == 7)
        self.assertTrue(_dev_in_builder(self.mock_builder, field='meta',
                                        match='test1'))
        id = _get_dev_id(self.mock_builder, field='meta', match='test1')
        device = _get_dev(self.mock_builder, id)
        for k in devices2add['devices'][0]:
            self.assertTrue(device[k] == devices2add['devices'][0][k])
        self.assertTrue(_dev_in_builder(self.mock_builder, field='meta',
                                        match='test2'))
        id = _get_dev_id(self.mock_builder, field='meta', match='test2')
        device = _get_dev(self.mock_builder, id)
        for k in devices2add['devices'][1]:
            self.assertTrue(device[k] == devices2add['devices'][1][k])

    def test_rb_add_object(self):

        def _dev_in_builder(builder, field='meta', match='test1'):
            for dev in builder.devs:
                if dev[field] == match:
                    return True
            return False

        def _get_dev_id(builder, field='meta', match='test1'):
            for dev in builder.devs:
                if dev[field] == match:
                    return dev['id']
            return False

        def _get_dev(builder, id):
            for dev in builder.devs:
                if dev['id'] == id:
                    return dev

        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        RingBuilder.get_builder = MagicMock(return_value=self.mock_builder)

        req = Request.blank('/ringbuilder/object/add', environ=req_env)
        req.method = 'POST'
        req.content_type = 'application/json'
        devices2add = {'devices': [{"weight": 15.0, "zone": 1,
                                    "ip": "1.1.1.1", "meta": "test1",
                                    "device": "sda", "port": 6010},
                                   {"weight": 15.0, "zone": 1,
                                    "ip": "1.1.1.1", "meta": "test2",
                                    "device": "sdb", "port": 6010}]}
        req.body = json.dumps(devices2add)
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK', [('X-Current-Hash',
                                                           'newhash')])
        bf = '/etc/swift/object.builder'
        RingBuilder.get_builder.assert_called_once_with(bf)
        self.assertTrue(len(self.mock_builder.devs) == 7)
        self.assertTrue(_dev_in_builder(self.mock_builder, field='meta',
                                        match='test1'))
        id = _get_dev_id(self.mock_builder, field='meta', match='test1')
        device = _get_dev(self.mock_builder, id)
        for k in devices2add['devices'][0]:
            self.assertTrue(device[k] == devices2add['devices'][0][k])
        self.assertTrue(_dev_in_builder(self.mock_builder, field='meta',
                                        match='test2'))
        id = _get_dev_id(self.mock_builder, field='meta', match='test2')
        device = _get_dev(self.mock_builder, id)
        for k in devices2add['devices'][1]:
            self.assertTrue(device[k] == devices2add['devices'][1][k])

    def test_rb_rebalance_account(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        builder = RingBuilder(1, 1, 1)
        for i in xrange(5):
            zone = i
            ipaddr = "1.1.1.1"
            port = 6010
            device_name = "sd%s" % i
            weight = 1.0
            meta = "meta for %s" % i
            next_dev_id = 0
            if builder.devs:
                next_dev_id = max(d['id'] for d in builder.devs if d) + 1
            builder.add_dev({'id': next_dev_id, 'zone': zone, 'ip': ipaddr,
                             'port': int(port), 'device': device_name,
                             'weight': weight, 'meta': meta})
        builder.get_ring = MagicMock()
        RingBuilder.get_builder = MagicMock(return_value=builder)
        req = Request.blank('/ringbuilder/account/rebalance', environ=req_env)
        req.method = 'POST'
        req.body = ""
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK',
                                               [('Content-Length', '52'),
                                                ('X-Current-Hash', 'newhash'),
                                                ('Content-Type',
                                                 'application/json')])
        mb_calls = mock_call('/etc/swift/account.ring.gz')
        self.app._make_backup.assert_has_calls(mb_calls, any_order=False)
        self.assertTrue(builder.get_ring.call_count == 1)
        bf = '/etc/swift/account.builder'
        self.app.write_builder.assert_has_calls([mock_call(builder, bf)])

    def test_rb_rebalance_container(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        builder = RingBuilder(1, 1, 1)
        for i in xrange(5):
            zone = i
            ipaddr = "1.1.1.1"
            port = 6010
            device_name = "sd%s" % i
            weight = 1.0
            meta = "meta for %s" % i
            next_dev_id = 0
            if builder.devs:
                next_dev_id = max(d['id'] for d in builder.devs if d) + 1
            builder.add_dev({'id': next_dev_id, 'zone': zone, 'ip': ipaddr,
                             'port': int(port), 'device': device_name,
                             'weight': weight, 'meta': meta})
        builder.get_ring = MagicMock()
        RingBuilder.get_builder = MagicMock(return_value=builder)
        req = Request.blank('/ringbuilder/container/rebalance',
                            environ=req_env)
        req.method = 'POST'
        req.body = ""
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK',
                                               [('Content-Length', '52'),
                                                ('X-Current-Hash', 'newhash'),
                                                ('Content-Type',
                                                 'application/json')])
        rf = '/etc/swift/container.ring.gz'
        bf = '/etc/swift/container.builder'
        mb_calls = mock_call(rf)
        self.app._make_backup.assert_has_calls(mb_calls, any_order=False)
        self.assertTrue(builder.get_ring.call_count == 1)
        self.app.write_builder.assert_has_calls([mock_call(builder, bf)])

    def test_rb_rebalance_object(self):
        req_env = {'HTTP_X_RING_BUILDER_KEY': 'something',
                   'HTTP_X_RING_BUILDER_LAST_HASH': 'newhash'}
        start_response = MagicMock(return_value="MOCKED")
        builder = RingBuilder(1, 1, 1)
        for i in xrange(5):
            zone = i
            ipaddr = "1.1.1.1"
            port = 6010
            device_name = "sd%s" % i
            weight = 1.0
            meta = "meta for %s" % i
            next_dev_id = 0
            if builder.devs:
                next_dev_id = max(d['id'] for d in builder.devs if d) + 1
            builder.add_dev({'id': next_dev_id, 'zone': zone, 'ip': ipaddr,
                             'port': int(port), 'device': device_name,
                             'weight': weight, 'meta': meta})
        builder.get_ring = MagicMock()
        RingBuilder.get_builder = MagicMock(return_value=builder)
        req = Request.blank('/ringbuilder/object/rebalance', environ=req_env)
        req.method = 'POST'
        req.body = ""
        req.content_length = int(len(req.body))
        resp = self.app(req.environ, start_response)
        start_response.assert_called_once_with('200 OK',
                                               [('Content-Length', '52'),
                                                ('X-Current-Hash', 'newhash'),
                                                ('Content-Type',
                                                 'application/json')])
        rf = '/etc/swift/object.ring.gz'
        bf = '/etc/swift/object.builder'
        mb_calls = mock_call(rf)
        self.app._make_backup.assert_has_calls(mb_calls, any_order=False)
        self.assertTrue(builder.get_ring.call_count == 1)
        self.app.write_builder.assert_has_calls([mock_call(builder, bf)])


class TestRingBuilderComponents(unittest.TestCase):

    def setUp(self):
        tb = FakedBuilder()
        self.mock_builder = tb.create_builder()
        self.real_search_devs = swift.common.ring.utils.search_devs
        self.search_result = {'id': 1, 'weight': 5}
        swift.common.ring.utils.search_devs = \
            MagicMock(return_value=self.search_result)
        self.real_lock_file = swift.common.utils.lock_file
        swift.common.utils.lock_file = MagicMock()
        self.real_pickle_dump = pickle.dump
        pickle.dump = MagicMock(return_value=True)
        self.real_pickle_load = pickle.load
        pickle.load = MagicMock(return_value=self.mock_builder)
        self.real_gzip = gzip.GzipFile
        gzip.GzipFile = MagicMock()
        self.real_mkdir = os.mkdir
        os.mkdir = MagicMock()
        from rbm import ring_builder

    def tearDown(self):
        pickle.load = self.real_pickle_load
        pickle.dump = self.real_pickle_dump
        gzip.Gzip = self.real_gzip
        swift.common.utils.lock_file = self.real_lock_file
        swift.common.ring.utils.search_devs = self.real_search_devs
        os.mkdir = self.real_mkdir

    def test_http_unauthorized_response_return(self):
        start_response = MagicMock(return_value="MOCKED")
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        result = self.app.http_unauthorized(start_response)
        self.assertTrue(result == [])
        start_response.assert_has_calls(mock_call('401 Unauthorized',
                                                  [('Content-Length', '0')]))

    def test_http_conflict_response_return(self):
        start_response = MagicMock(return_value="MOCKED")
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        result = self.app.http_conflict(start_response, 'test content')
        self.assertTrue(result == ['test content\r\n'])
        start_response.assert_has_calls(mock_call('409 Conflict',
                                                  [('Content-Length', '14'),
                                                   ('Content-Type',
                                                    'text/plain')]))

    def test_http_bad_request_response_return(self):
        start_response = MagicMock(return_value="MOCKED")
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        result = self.app.http_bad_request(start_response, 'test content')
        self.assertTrue(result == ['test content\r\n'])
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '14'),
                                                   ('Content-Type',
                                                    'text/plain')]))

    def test_http_ok_response_return(self):
        start_response = MagicMock(return_value="MOCKED")
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        result = self.app.http_ok(start_response, ringhash='ringhash')
        self.assertTrue(result == [])
        start_response.assert_has_calls(mock_call('200 OK', [('X-Current-Hash',
                                                              'ringhash')]))
        start_response.reset_mock()
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        result = self.app.http_ok(start_response)
        start_response.assert_has_calls(mock_call('200 OK',
                                                  [('Content-Length', '0')]))

    def test_http_internal_server_response_return(self):
        start_response = MagicMock(return_value="MOCKED")
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        result = self.app.http_internal_server_error(start_response,
                                                     'test content')
        self.assertTrue(result == ['test content\r\n'])
        start_response.assert_has_calls(mock_call('500 Internal Server Error',
                                                  [('Content-Length', '14'),
                                                   ('Content-Type',
                                                    'text/plain')]))

    def test_return_response(self):
        start_response = MagicMock(return_value="MOCKED")
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        self.app._log_request = MagicMock()
        result = self.app.return_response(success=True,
                                          current_hash="thehash",
                                          content=None,
                                          start_response=start_response,
                                          env=None)
        self.assertTrue(result == [])
        start_response.assert_has_calls(mock_call('200 OK',
                                                  [('X-Current-Hash',
                                                    'thehash')]))
        start_response.reset_mock()
        result = self.app.return_response(success=True,
                                          current_hash="thehash",
                                          content=[],
                                          start_response=start_response,
                                          env=None)
        self.assertTrue(result == ['[]'])
        start_response.assert_has_calls(mock_call('200 OK',
                                                  [('Content-Length', '2'),
                                                   ('X-Current-Hash',
                                                    'thehash'),
                                                   ('Content-Type',
                                                    'application/json')]))
        start_response.reset_mock()
        result = self.app.return_response(success=True,
                                          current_hash="thehash2",
                                          content="something",
                                          start_response=start_response,
                                          env=None)
        self.assertTrue(result == ['"something"'])
        start_response.assert_has_calls(mock_call('200 OK',
                                                  [('Content-Length', '11'),
                                                   ('X-Current-Hash',
                                                    'thehash2'),
                                                   ('Content-Type',
                                                    'application/json')]))
        start_response.reset_mock()
        result = self.app.return_response(success=False,
                                          current_hash="thehash3",
                                          content="something",
                                          start_response=start_response,
                                          env=None)
        self.assertTrue(result == ['something\r\n'])
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '11'),
                                                   ('Content-Type',
                                                    'text/plain')]))

    def test_get(self):
        start_response = MagicMock(return_value="MOCKED")
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        #test invalid
        result = self.app.get_or_head({'PATH_INFO': '/ring/file'},
                                      start_response)
        self.assertTrue(result == ['Resource could not be found.\r\n'])
        start_response.assert_has_calls(mock_call('404 Not Found',
                                                  [('Content-Length', '30'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()
        #test valid get /ring/valid
        self.app.return_static_file = MagicMock(return_value="MOCKED")
        result = self.app.get_or_head({'PATH_INFO': '/ring/object.ring.gz',
                                       'REQUEST_METHOD': 'GET'},
                                      start_response)
        self.assertTrue(result == 'MOCKED')
        self.assertTrue('/etc/swift/object.ring.gz' in
                        self.app.return_static_file.call_args[0])
        result = self.app.get_or_head({'PATH_INFO': '/ring/object.builder',
                                       'REQUEST_METHOD': 'GET'},
                                      start_response)
        self.assertTrue(result == 'MOCKED')
        self.assertTrue('/etc/swift/object.builder' in
                        self.app.return_static_file.call_args[0])
        start_response.reset_mock()
        #test listing
        self.app.list_devices = MagicMock(return_value="MOCKED")
        result = self.app.get_or_head({'PATH_INFO': '/ringbuilder/object/list',
                                       'REQUEST_METHOD': 'GET'},
                                      start_response)
        self.assertTrue(result == 'MOCKED')
        self.assertTrue('object' in self.app.list_devices.call_args[0])
        start_response.reset_mock()
        #test exception
        self.app.list_devices.reset_mock()
        self.app.list_devices.side_effect = Exception('oops')
        result = self.app.get_or_head({'PATH_INFO': '/ringbuilder/object/list',
                                       'REQUEST_METHOD': 'GET'},
                                      start_response)
        self.assertTrue(result == ['oops\r\n'])
        start_response.assert_has_calls(mock_call('500 Internal Server Error',
                                                  [('Content-Length', '6'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()
        #test exception
        self.app.list_devices.reset_mock()
        self.app.list_devices.side_effect = LockTimeout
        result = self.app.get_or_head({'PATH_INFO': '/ringbuilder/object/list',
                                       'REQUEST_METHOD': 'GET'},
                                      start_response)
        self.assertTrue(result == ['Ring locked.\r\n'])
        start_response.assert_has_calls(mock_call('409 Conflict',
                                                  [('Content-Length', '14'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()

    def test_ring_head(self):
        start_response = MagicMock(return_value="MOCKED")
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        self.app.handle_head = MagicMock(return_value="MOCKED")
        #test invalid
        result = self.app.ring_or_builder_head({'PATH_INFO': '/ring/f.builder',
                                                'REQUEST_METHOD': 'HEAD'},
                                               start_response)
        self.assertTrue(result == ['Invalid ring type or path: f\r\n'])
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '30'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()
        #test valid
        self.app.handle_head.reset_mock()
        result = self.app.ring_or_builder_head({'PATH_INFO':
                                                '/ring/object.ring.gz',
                                                'REQUEST_METHOD': 'HEAD'},
                                               start_response)
        self.assertTrue(result == 'MOCKED')
        start_response.reset_mock()
        #test exception
        self.app.handle_head.reset_mock()
        self.app.handle_head.side_effect = Exception('oops')
        result = self.app.ring_or_builder_head({'PATH_INFO':
                                                '/ring/object.ring.gz',
                                                'REQUEST_METHOD': 'HEAD'},
                                               start_response)
        self.assertTrue(result == ['oops\r\n'])
        start_response.assert_has_calls(mock_call('500 Internal Server Error',
                                                  [('Content-Length', '6'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()
        #test exception
        self.app.handle_head.reset_mock()
        self.app.handle_head.side_effect = LockTimeout
        result = self.app.ring_or_builder_head({'PATH_INFO':
                                                '/ring/object.ring.gz',
                                                'REQUEST_METHOD': 'HEAD'},
                                               start_response)
        self.assertTrue(result == ['object.ring.gz locked.\r\n'])
        start_response.assert_has_calls(mock_call('409 Conflict',
                                                  [('Content-Length', '24'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()

    def test_builder_head(self):
        start_response = MagicMock(return_value="MOCKED")
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        self.app.handle_head = MagicMock(return_value="MOCKED")
        #test invalid
        result = self.app.ring_or_builder_head({'PATH_INFO':
                                                '/ringbuilder/file.invalid',
                                                'REQUEST_METHOD': 'HEAD'},
                                               start_response)
        expected_result = ['Invalid ringbuilder type or path: file\r\n']
        self.assertTrue(result == expected_result)
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '40'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()
        #test valid
        self.app.handle_head.reset_mock()
        result = self.app.ring_or_builder_head({'PATH_INFO':
                                                '/ringbuilder/object.builder',
                                                'REQUEST_METHOD': 'HEAD'},
                                               start_response)
        self.assertTrue(result == 'MOCKED')
        start_response.reset_mock()
        #test exception
        self.app.handle_head.reset_mock()
        self.app.handle_head.side_effect = Exception('oops')
        result = self.app.ring_or_builder_head({'PATH_INFO':
                                                '/ringbuilder/object.builder',
                                                'REQUEST_METHOD': 'HEAD'},
                                               start_response)
        self.assertTrue(result == ['oops\r\n'])
        start_response.assert_has_calls(mock_call('500 Internal Server Error',
                                                  [('Content-Length', '6'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()
        #test exception
        self.app.handle_head.reset_mock()
        self.app.handle_head.side_effect = LockTimeout
        result = self.app.ring_or_builder_head({'PATH_INFO':
                                                '/ringbuilder/object.builder',
                                                'REQUEST_METHOD': 'HEAD'},
                                               start_response)
        self.assertTrue(result == ['object.builder locked.\r\n'])
        start_response.assert_has_calls(mock_call('409 Conflict',
                                                  [('Content-Length', '24'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()

    def test_post(self):
        start_response = MagicMock(return_value="MOCKED")
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        self.app.handle_post = MagicMock(return_value="MOCKED")
        #test invalid
        result = self.app.post({'PATH_INFO': '/ringbuilder/file/add'},
                               start_response, 'something')
        self.assertTrue(result == ['Invalid builder type.\r\n'])
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '23'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()
        #test valid
        self.app.handle_post.reset_mock()
        result = self.app.post({'PATH_INFO': '/ringbuilder/object/add'},
                               start_response, 'something')
        self.assertTrue(result == 'MOCKED')
        start_response.reset_mock()
        #test exception
        self.app.handle_post.reset_mock()
        self.app.handle_post.side_effect = Exception('oops')
        result = self.app.post({'PATH_INFO': '/ringbuilder/object/add'},
                               start_response, 'something')
        self.assertTrue(result == ['oops\r\n'])
        start_response.assert_has_calls(mock_call('500 Internal Server Error',
                                                  [('Content-Length', '6'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()
        #test locktimeout
        self.app.handle_post.reset_mock()
        self.app.handle_post.side_effect = LockTimeout
        result = self.app.post({'PATH_INFO': '/ringbuilder/object/add'},
                               start_response, 'something')
        self.assertTrue(result == ['Builder locked.\r\n'])
        start_response.assert_has_calls(mock_call('409 Conflict',
                                                  [('Content-Length', '17'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()
        #test ringbuilderchanged
        self.app.handle_post.reset_mock()
        self.app.handle_post.side_effect = RingFileChanged
        result = self.app.post({'PATH_INFO': '/ringbuilder/object/add'},
                               start_response, 'something')
        self.assertTrue(result == ['Builder md5sum differs\r\n'])
        start_response.assert_has_calls(mock_call('409 Conflict',
                                                  [('Content-Length', '24'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()

    def test_handle_post(self):
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        self.app.add_to_ring = MagicMock(return_value="MOCKED")
        #test no content type
        builder_type = "object"
        target = "add"
        env = {'PATH_INFO': '/ringbuilder/object/add'}
        start_response = MagicMock(return_value="MOCKED")
        body = 'SOMETHING'
        result = self.app.handle_post(builder_type, target, env,
                                      start_response, body)
        self.assertTrue(result == ['Bad Request\r\n'])
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '13'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()
        #test content type - bad json
        builder_type = "object"
        target = "add"
        env = {'PATH_INFO': '/ringbuilder/object/add',
               'CONTENT_TYPE': 'application/json'}
        start_response = MagicMock(return_value="MOCKED")
        body = 'SOMETHING'
        result = self.app.handle_post(builder_type, target, env,
                                      start_response, body)
        self.assertTrue(result == ['Malformed json.\r\n'])
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '17'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()
        #test content type - other than json
        builder_type = "object"
        target = "add"
        env = {'PATH_INFO': '/ringbuilder/object/rebalance',
               'CONTENT_TYPE': 'application/server-monkey'}
        start_response = MagicMock(return_value="MOCKED")
        body = 'SOMETHING'
        result = self.app.handle_post(builder_type, target, env,
                                      start_response, body)
        self.assertTrue(result == ['Bad Request\r\n'])
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '13'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        start_response.reset_mock()

    def test_add_to_ring_errors(self):
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        self.app._get_md5sum = MagicMock(return_value="currenthash")
        self.app.verify_current_hash = MagicMock(return_value="currenthash")
        #self.app.get_builder = MagicMock(return_value=self.mock_builder)
        self.app.is_existing_dev = MagicMock(return_value=True)
        self.app._log_request = MagicMock()
        start_response = MagicMock(return_value="MOCKED")
        builder_type = 'object'
        body = {'devices': [{"weight": 1.0, "zone": 1, "ip": "1.1.1.1",
                             "meta": "meta for 1", "device": "sd1",
                             "port": 6010}]}
        lasthash = 'currenthash'
        env = None
        result = self.app.add_to_ring(builder_type, body, lasthash,
                                      start_response, env)
        self.assertTrue(result == ['Ring remains unchanged.\r\n'])
        #malformed body
        body = {'devices': 'oops'}
        lasthash = 'currenthash'
        result = self.app.add_to_ring(builder_type, body, lasthash,
                                      start_response, env)
        self.assertTrue(result == ['Malformed request.\r\n'])

    def test_change_meta_errors(self):
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        self.app._get_md5sum = MagicMock(return_value="currenthash")
        self.app.verify_current_hash = MagicMock(return_value="currenthash")
        #self.app.get_builder = MagicMock(return_value=self.mock_builder)
        self.app.is_existing_dev = MagicMock(return_value=True)
        self.app.write_builder = MagicMock(return_value="newhash")
        self.app._log_request = MagicMock()
        start_response = MagicMock(return_value="MOCKED")
        builder_type = 'object'
        body = {"99": "meta for 99"}
        lasthash = 'currenthash'
        env = None
        result = self.app.change_meta(builder_type, body, lasthash,
                                      start_response, env)
        self.assertTrue(result == ['Invalid dev id 99.\r\n'])
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '20'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        #malformed body
        start_response.reset_mock()
        body = {'devices': 'oops'}
        lasthash = 'currenthash'
        result = self.app.change_meta(builder_type, body, lasthash,
                                      start_response, env)
        msg = ["invalid literal for int() with base 10: 'devices'\r\n"]
        self.assertTrue(result == msg)
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '51'),
                                                   ('Content-Type',
                                                    'text/plain')]))

    def test_change_weight_errors(self):
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        self.app._get_md5sum = MagicMock(return_value="currenthash")
        self.app.verify_current_hash = MagicMock(return_value="currenthash")
        #self.app.get_builder = MagicMock(return_value=self.mock_builder)
        self.app.is_existing_dev = MagicMock(return_value=True)
        self.app.write_builder = MagicMock(return_value="newhash")
        self.app._log_request = MagicMock()
        start_response = MagicMock(return_value="MOCKED")
        builder_type = 'object'
        body = {"99": 5.0}
        lasthash = 'currenthash'
        env = None
        result = self.app.change_weight(builder_type, body, lasthash,
                                        start_response, env)
        self.assertTrue(result == ['Invalid dev id 99.\r\n'])
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '20'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        #malformed body
        start_response.reset_mock()
        body = {'1': 'oops'}
        lasthash = 'currenthash'
        result = self.app.change_weight(builder_type, body, lasthash,
                                        start_response, env)
        self.assertTrue('400 Bad Request' in start_response.call_args[0])

    def test_remove_devs_errors(self):
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        self.app._get_md5sum = MagicMock(return_value="currenthash")
        self.app.verify_current_hash = MagicMock(return_value="currenthash")
        #self.app.get_builder = MagicMock(return_value=self.mock_builder)
        self.app.is_existing_dev = MagicMock(return_value=True)
        self.app.write_builder = MagicMock(return_value="newhash")
        self.app._log_request = MagicMock()
        start_response = MagicMock(return_value="MOCKED")
        builder_type = 'object'
        body = [1, 99]
        lasthash = 'currenthash'
        env = None
        result = self.app.remove_devs(builder_type, body, lasthash,
                                      start_response, env)
        self.assertTrue(result == ['Invalid dev id 99.\r\n'])
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '20'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        #malformed body
        start_response.reset_mock()
        body = {'not a': 'list'}
        lasthash = 'currenthash'
        result = self.app.remove_devs(builder_type, body, lasthash,
                                      start_response, env)
        self.assertTrue(result == ['Malformed request.\r\n'])
        start_response.assert_has_calls(mock_call('400 Bad Request',
                                                  [('Content-Length', '20'),
                                                   ('Content-Type',
                                                    'text/plain')]))
        #malformed body with non int dev id
        start_response.reset_mock()
        body = ['1', 'oops']
        lasthash = 'currenthash'
        result = self.app.remove_devs(builder_type, body, lasthash,
                                      start_response, env)
        self.assertTrue('400 Bad Request' in start_response.call_args[0])

    def test_make_backup(self):
        shutil.copy = MagicMock()
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        self.app._get_md5sum = MagicMock(return_value="currenthash")
        #self.app.get_builder = MagicMock(return_value=self.mock_builder)
        self.app.is_existing_dev = MagicMock(return_value=True)
        self.app.write_builder = MagicMock(return_value="newhash")
        builder_type = 'object'
        result = self.app._make_backup('something')
        self.assertTrue(result is None)
        os.mkdir.assert_has_calls(mock_call('/etc/swift/backups'))
        #oserror dir exists
        os.mkdir.reset_mock()
        os.mkdir.side_effect = OSError(errno.EEXIST, 'something')
        result = self.app._make_backup('something')
        self.assertTrue(shutil.copy.call_count == 2)
        os.mkdir.assert_has_calls(mock_call('/etc/swift/backups'))
        #oserror dir exists
        os.mkdir.reset_mock()
        os.mkdir.side_effect = OSError(2, 'oops')
        self.assertRaises(OSError, self.app._make_backup, 'something')

    def test_verify_current_hash_bad_hash(self):
        from rbm import ring_builder
        self.app = ring_builder.RingBuilderMiddleware(FakeApp(), {'key': 'a'})
        self.app._get_md5sum = MagicMock(return_value="currenthash")
        #self.app.get_builder = MagicMock(return_value=self.mock_builder)
        self.assertRaises(RingFileChanged, self.app.verify_current_hash, 'a',
                          'notvalid')


if __name__ == '__main__':
    unittest.main()
