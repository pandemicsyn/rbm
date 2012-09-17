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


import os
import shutil
from hashlib import md5
from errno import EEXIST
import cPickle as pickle
from webob import Request
from eventlet import sleep
from urllib import quote, unquote
from time import gmtime, strftime, time
from os.path import basename, join as pathjoin, getsize
from swift.common.ring import RingBuilder
from swift.common.utils import split_path, get_logger, lock_file
from swift.common.exceptions import LockTimeout, RingBuilderError, \
    RingValidationError
try:
    import simplejson as json
except ImportError:
    import json


class FileIterable(object):
    def __init__(self, filename):
        self.filename = filename

    def __iter__(self):
        return FileIterator(self.filename)


class FileIterator(object):

    chunk_size = 4096

    def __init__(self, filename):
        self.filename = filename
        self.fileobj = open(self.filename, 'rb')

    def __iter__(self):
        return self

    def next(self):
        chunk = self.fileobj.read(self.chunk_size)
        if not chunk:
            raise StopIteration
        return chunk

    __next__ = next

class RingFileChanged(Exception):
        pass

class RingBuilderMiddleware(object):

    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.logger = get_logger(conf, log_route='ring_builder')
        self.backup_dir = conf.get('backup_dir', '/etc/swift/backups')
        self.acct_builder = conf.get('account_builder', 'account.builder')
        self.cont_builder = conf.get('container_builder', 'container.builder')
        self.obj_builder = conf.get('object_builder', 'object.builder')
        self.acct_ring = conf.get('account_ring', 'account.ring.gz')
        self.cont_ring = conf.get('container_ring', 'container.ring.gz')
        self.obj_ring = conf.get('object_ring', 'object.ring.gz')
        self.bf_path = {'account': pathjoin(self.swift_dir, self.acct_builder),
                        'container': pathjoin(self.swift_dir,
                                              self.cont_builder),
                        'object': pathjoin(self.swift_dir, self.obj_builder)}
        self.rf_path = {'account': pathjoin(self.swift_dir, self.acct_ring),
                        'container': pathjoin(self.swift_dir, self.cont_ring),
                        'object': pathjoin(self.swift_dir, self.obj_ring)}
        self.key = conf['key']

    def _log_request(self, env, response_status_int):
        """
        Used when a request might not be logged by the underlying
        WSGI application, but we'd still like to record what
        happened. An early 401 Unauthorized is a good example of
        this.

        :param env: The WSGI environment for the request.
        :param response_status_int: The HTTP status we'll be replying
                                    to the request with.
        """
        the_request = quote(unquote(env.get('PATH_INFO') or '/'))
        if env.get('QUERY_STRING'):
            the_request = the_request + '?' + env['QUERY_STRING']
        client = env.get('HTTP_X_CLUSTER_CLIENT_IP')
        if not client and 'HTTP_X_FORWARDED_FOR' in env:
            # remote host for other lbs
            client = env['HTTP_X_FORWARDED_FOR'].split(',')[0].strip()
        if not client:
            client = env.get('REMOTE_ADDR')
        self.logger.info(' '.join(quote(str(x)) for x in (
            client or '-',
            env.get('REMOTE_ADDR') or '-',
            strftime('%d/%b/%Y/%H/%M/%S', gmtime()),
            env.get('REQUEST_METHOD') or 'GET',
            the_request,
            env.get('SERVER_PROTOCOL') or '1.0',
            response_status_int,
            env.get('HTTP_REFERER') or '-',
            (env.get('HTTP_USER_AGENT') or '-') + ' ring_builder',
            env.get('HTTP_X_AUTH_TOKEN') or '-',
            '-',
            '-',
            '-',
            env.get('swift.trans_id') or '-',
            '-',
            '-',
        )))

    @staticmethod
    def _get_md5sum(filename):
        """Get the md5sum of file

        :params filename: file to obtain the md5sum of
        :returns: hex digest of file
        """
        md5sum = md5()
        with open(filename, 'rb') as tfile:
            block = tfile.read(4096)
            while block:
                md5sum.update(block)
                block = tfile.read(4096)
        return md5sum.hexdigest()

    def _make_backup(self, filename):
        """ Create a backup of the current builder file

        :params filename: The file to backup
        """
        try:
            os.mkdir(self.backup_dir)
        except OSError, err:
            if err.errno != EEXIST:
                raise
        backup = pathjoin(self.backup_dir, '%d.' %
                          time() + basename(filename))
        shutil.copy(filename, backup)
        self.logger.info(_('Backed up %s to %s (%s)' %
                        (filename, backup, self._get_md5sum(backup))))

    def _is_existing_dev(self, builder, ipaddr, port, device_name):
        """ Check if a device is currently present in the builder

        :params builder: builder instance to use
        :params ip: ip to check for
        :params port: port to check for
        :params device_name: device_name to check for
        :returns: True or False
        """
        for dev in builder.devs:
            if dev is None:
                continue
            if dev['ip'] == ipaddr and dev['port'] == port and \
                    dev['device'] == device_name:
                return True
        return False

    def _add_device(self, builder, zone, ipaddr, port, device_name, weight,
                    meta):
        """ Add a device to the builder instance

        :params builder: builder instance to use
        :params zone: zone of new device
        :params ipaddr: ip of new device
        :params port: port of new device
        :params device_name: device_name of new device
        :params weight: weight of new device
        :params meta: meta info of new device
        """
        next_dev_id = 0
        if builder.devs:
            next_dev_id = max(d['id'] for d in builder.devs if d) + 1
        builder.add_dev({'id': next_dev_id, 'zone': zone, 'ip': ipaddr,
                         'port': int(port), 'device': device_name,
                         'weight': weight, 'meta': meta})

    def return_static_file(self, filename, start_response, env):
        """ lock and serve a static file to the client from disk

        :params filename: the file to serve and who's md5sum to use
                          for the X-Current-Hash header.
        :params start_response: start_response object
        :returns: iterator for reading the file from disk.
        """
        with lock_file(filename, timeout=1, unlink=False):
            filehash = self._get_md5sum(filename)
            self._log_request(env, 200)
            start_response('200 OK', [('Content-Length', getsize(filename)),
                                      ('X-Current-Hash', filehash),
                                      ('Content-Type',
                                       'application/octet-stream')])
            return FileIterable(filename)

    def write_builder(self, builder, builder_file):
        """Write out RingBuilder instance

        :params builder: builder instance to use
        :params builder_file: path to builder_file
        :returns: md5sum of the newly written builder
        """
        self._make_backup(builder_file)
        pickle.dump(builder.to_dict(), open(builder_file, 'wb'), protocol=2)
        newmd5 = self._get_md5sum(builder_file)
        self.logger.info('Wrote %s (%s)' % (builder_file, newmd5))
        return newmd5

    def verify_current_hash(self, builder_file, lasthash):
        """ Verify a builder file matches the provided md5sum

        :params builder_file: builder file to check
        :params lasthash: hash to check against
        :raises: RingFileChanged Exception if md5sum differs
        """
        current_md5sum = self._get_md5sum(builder_file)
        if lasthash != current_md5sum:
            raise RingFileChanged('%s builder md5sum differs' %
                                  basename(builder_file))

    def rebalance(self, builder_type, lasthash, start_response, env):
        """ rebalance a ring

            note: rebalance doesn't yield.
        """
        with lock_file(self.bf_path[builder_type], timeout=1, unlink=False):
            self.verify_current_hash(self.bf_path[builder_type], lasthash)
            builder = RingBuilder.get_builder(self.bf_path[builder_type])
            devs_changed = builder.devs_changed
            try:
                last_balance = builder.get_balance()
                parts, balance = builder.rebalance()
            except RingBuilderError, err:
                self.logger.exception(_("Error during ring validation."))
                return self.return_response(False, None, err.message,
                                            start_response, env)
            if not parts:
                msg = 'Either none need to be assigned or none can be due ' \
                      'to min_part_hours [%s].' % builder.min_part_hours
                self.logger.error(_(msg))
                return self.return_response(False, None, msg, start_response,
                                            env)
            if not devs_changed and abs(last_balance - balance) < 1:
                msg = 'Refusing to save rebalance. Did not change at least 1%.'
                self.logger.error(_(msg))
                return self.return_response(False, None, msg, start_response,
                                            env)
            try:
                builder.validate()
            except RingValidationError, err:
                self.logger.exception(_("Error during ring validation."))
                return self.return_response(False, None, err.message,
                                            start_response, env)
            self.logger.info(_('Reassigned %d (%.02f%%) partitions. Balance is'
                               ' %.02f.' % (parts,
                                            100.0 * parts / builder.parts,
                                            balance)))
            if balance > 5:
                self.logger.info(_('Balance of %.02f indicates you should '
                                   'push this ring, wait %d hours and '
                                   'rebalance/repush.' % (balance,
                                   builder.min_part_hours)))
            newmd5 = self.write_builder(builder, self.bf_path[builder_type])
            ring_file = self.rf_path[builder_type]
            self._make_backup(ring_file)
            builder.get_ring().save(ring_file)
            self.logger.info(_('Wrote new ring file %s (%s)' %
                               (ring_file, self._get_md5sum(ring_file))))
            return self.return_response(True, newmd5, {'balance': balance,
                                                       'reassigned': parts,
                                                       'partitions':
                                                       builder.parts},
                                        start_response, env)

    def list_devices(self, builder_type, start_response, env):
        """ list ALL devices in the ring

        :params builder_type: the builder_type to use when loading the builder
        :returns: list of boolean status, md5sum of the current ring, and all
                  builder.devs
        """
        with lock_file(self.bf_path[builder_type], timeout=1, unlink=False):
            builder = RingBuilder.get_builder(self.bf_path[builder_type])
            current_md5sum = self._get_md5sum(self.bf_path[builder_type])
            return self.return_response(True, current_md5sum, builder.devs,
                                        start_response, env)

    def search(self, builder_type, search_pattern, start_response, env):
        """ search the builder for devices matching search pattern

        :params builder_type: the builder_type to use when loading the builder
        :params search_values: the value to search for
        :returns: list of boolean status, md5sum of current builder
                  file on disk, and error message or dict of matched devices.
        """
        with lock_file(self.bf_path[builder_type], timeout=1, unlink=False):
            builder = RingBuilder.get_builder(self.bf_path[builder_type])
            try:
                search_result = builder.search_devs(str(search_pattern))
                return self.return_response(True, self._get_md5sum(
                                            self.bf_path[builder_type]),
                                            search_result,
                                            start_response, env)
            except ValueError:
                return self.return_response(False, self._get_md5sum(
                                            self.bf_path[builder_type]),
                                            'Invalid search term',
                                            start_response, env)

    def remove_devs(self, builder_type, devices, lasthash, start_response,
                    env):
        """ remove devices from the builder

        :params builder_type: the builder_type to use when loading the builder
        :params devices: list of device ids to be removed.
        :params lasthash: the hash to use when verifying state
        """
        with lock_file(self.bf_path[builder_type], timeout=1, unlink=False):
            self.verify_current_hash(self.bf_path[builder_type], lasthash)
            builder = RingBuilder.get_builder(self.bf_path[builder_type])
            if not isinstance(devices, list):
                return self.return_response(False, lasthash,
                                            'Malformed request.',
                                            start_response, env)
            for dev_id in devices:
                sleep()  # so we don't starve/block
                try:
                    builder.remove_dev(int(dev_id))
                except (IndexError, TypeError):
                    return self.return_response(False, lasthash,
                                                'Invalid dev id %s.' % dev_id,
                                                start_response, env)
                except RingBuilderError as err:
                    return self.return_response(False, lasthash,
                                                'Error removing %s - %s.' %
                                                (dev_id, err),
                                                start_response, env)
                except ValueError as err:
                    return self.return_response(False, lasthash, str(err),
                                                start_response, env)
            newmd5 = self.write_builder(builder, self.bf_path[builder_type])
            return self.return_response(True, newmd5, None, start_response,
                                        env)

    def change_weight(self, builder_type, dev_weights, lasthash,
                      start_response, env):
        """ Change weight of devices

        :param builder_type: the builder_type to use when loading the builder
        :param dev_weights: a dict of device id and weight
        :param lasthash: the hash to use when verifying state
        """
        with lock_file(self.bf_path[builder_type], timeout=1, unlink=False):
            self.verify_current_hash(self.bf_path[builder_type], lasthash)
            builder = RingBuilder.get_builder(self.bf_path[builder_type])
            for dev_id in dev_weights:
                sleep()  # so we don't starve/block
                try:
                    builder.set_dev_weight(int(dev_id),
                                           float(dev_weights[dev_id]))
                except (IndexError, TypeError):
                    return self.return_response(False, lasthash,
                                                'Invalid dev id %s.' % dev_id,
                                                start_response, env)
                except ValueError as err:
                    return self.return_response(False, lasthash, str(err),
                                                start_response, env)
            newmd5 = self.write_builder(builder, self.bf_path[builder_type])
            return self.return_response(True, newmd5, None, start_response,
                                        env)

    def change_meta(self, builder_type, dev_meta, lasthash, start_response,
                    env):
        """ Change meta info for devices

        :param builder_type: the builder_type to use when loading the builder
        :param dev_meta: a dict of device id and meta info
        :param lasthash: the hash to use when verifying state
        """
        with lock_file(self.bf_path[builder_type], timeout=1, unlink=False):
            self.verify_current_hash(self.bf_path[builder_type], lasthash)
            builder = RingBuilder.get_builder(self.bf_path[builder_type])
            try:
                modified = False
                for dev_id in dev_meta:
                    sleep()  # so we don't starve/block
                    for device in builder.devs:
                        if not device:
                            continue
                        if device['id'] == int(dev_id):
                            modified = True
                            device['meta'] = '%s' % dev_meta[dev_id]
                if modified:
                    newmd5 = self.write_builder(builder,
                                                self.bf_path[builder_type])
                    return self.return_response(True, newmd5, None,
                                                start_response, env)
                else:
                    return self.return_response(False, lasthash,
                                                'Invalid dev id %s.' % dev_id,
                                                start_response, env)
            except ValueError as err:
                return self.return_response(False, lasthash, str(err),
                                            start_response, env)

    def add_to_ring(self, builder_type, body, lasthash, start_response, env):
        """ Handle a add device post """
        with lock_file(self.bf_path[builder_type], timeout=1, unlink=False):
            self.verify_current_hash(self.bf_path[builder_type], lasthash)
            builder = RingBuilder.get_builder(self.bf_path[builder_type])
            ring_modified = False
            try:
                for device in body['devices']:
                    sleep()  # so we don't starve/block
                    if not self._is_existing_dev(builder, device['ip'],
                                                 int(device['port']),
                                                 device['device']):
                        self._add_device(builder, int(device['zone']),
                                         device['ip'], int(device['port']),
                                         device['device'],
                                         float(device['weight']),
                                         device['meta'])
                        ring_modified = True
            except (AttributeError, KeyError, ValueError, TypeError) as err:
                return self.return_response(False, lasthash,
                                            "Malformed request.",
                                            start_response, env)
            if ring_modified:
                newmd5 = self.write_builder(builder,
                                            self.bf_path[builder_type])
                return self.return_response(True, newmd5, None,
                                            start_response, env)
            else:
                return self.return_response(False, lasthash,
                                            'Ring remains unchanged.',
                                            start_response, env)

    def handle_post(self, builder_type, target, env, start_response, body):
        """ Prase and handle a ring builder post request"""
        if 'CONTENT_TYPE' in env:
            if env['CONTENT_TYPE'] == 'application/json':
                try:
                    content = json.loads(body)
                except ValueError:
                    self._log_request(env, 400)
                    return self.http_bad_request(start_response,
                                                 'Malformed json.')
            else:
                content = {}
        else:
            if target != 'rebalance':
                self._log_request(env, 400)
                return self.http_bad_request(start_response, 'Bad Request')
        if 'HTTP_X_RING_BUILDER_LAST_HASH' in env:
            lasthash = env['HTTP_X_RING_BUILDER_LAST_HASH']
        else:
            lasthash = None
        if target == 'add' and 'devices' in content and lasthash:
            return self.add_to_ring(builder_type, json.loads(body), lasthash,
                                    start_response, env)
        elif target == 'remove' and 'devices' in content and lasthash:
            return self.remove_devs(builder_type, content['devices'], lasthash,
                                    start_response, env)
        elif target == 'weight' and 'devices' in content and lasthash:
            return self.change_weight(builder_type, content['devices'],
                                      lasthash, start_response, env)
        elif target == 'meta' and 'devices' in content and lasthash:
            return self.change_meta(builder_type, content['devices'], lasthash,
                                    start_response, env)
        elif target == 'rebalance' and lasthash:
            return self.rebalance(builder_type, lasthash, start_response, env)
        elif target == 'search' and 'value' in content:
            return self.search(builder_type, content['value'], start_response,
                               env)
        else:
            self._log_request(env, 400)
            return self.http_bad_request(start_response, 'Bad Request')

    def handle_head(self, target_file, start_response, env):
        """handle a head request. at this point it simply obtains the targets
        md5sum.

        :params target_file: File whos md5sum to obtain.
        :returns: list of boolean status, md5sum of the current ring, and all
                  builder.devs
        """
        with lock_file(target_file, unlink=False):
            current_hash = self._get_md5sum(target_file)
            return self.return_response(True, current_hash, None,
                                        start_response, env)

    def post(self, env, start_response, body):
        """handle all post requests"""
        builder_type, target = split_path(env['PATH_INFO'], 3, 3, True)[1:]
        if builder_type not in ['account', 'container', 'object']:
            self._log_request(env, 400)
            return self.http_bad_request(start_response,
                                         'Invalid builder type.')
        try:
            return self.handle_post(builder_type, target, env,
                                    start_response, body)
        except RingFileChanged:
            self._log_request(env, 409)
            return self.http_conflict(start_response, 'Builder md5sum differs')
        except LockTimeout:
            self._log_request(env, 409)
            return self.http_conflict(start_response, 'Builder locked.')
        except Exception as err:
            self.logger.exception(_('error on builder post'))
            self._log_request(env, 500)
            return self.http_internal_server_error(start_response, str(err))

    def ring_or_builder_head(self, env, start_response):
        """handle heads for /ring/ and /ringbuilder/"""
        path_prefix, path = split_path(env['PATH_INFO'], 1, 2, True)
        target, file_type = path.split('.', 1)
        if target not in ['account', 'container', 'object']:
            self._log_request(env, 400)
            return self.http_bad_request(start_response,
                                         'Invalid %s type or path: %s'
                                         % (path_prefix, target))
        try:
            if path_prefix == 'ring' and file_type == 'ring.gz':
                return self.handle_head(self.rf_path[target],
                                        start_response, env)
            elif path_prefix == 'ringbuilder' and file_type == 'builder':
                return self.handle_head(self.bf_path[target],
                                        start_response, env)
            else:
                self._log_request(env, 400)
                return self.http_bad_request(start_response,
                                             'Invalid %s type for path: %s.%s'
                                             % (path_prefix, target,
                                                file_type))
        except LockTimeout:
            self._log_request(env, 409)
            return self.http_conflict(start_response, '%s locked.' % path)
        except Exception as err:
            self.logger.exception(_('error on %s head' % path))
            self._log_request(env, 500)
            return self.http_internal_server_error(start_response, str(err))

    def get_or_head(self, env, start_response):
        """handle all get requests"""
        path_prefix, path = split_path(env['PATH_INFO'], 2, 2, True)
        allowed_files = [self.acct_builder, self.cont_builder,
                         self.obj_builder, self.acct_ring, self.cont_ring,
                         self.obj_ring]
        allowed_paths = ['account/list', 'container/list', 'object/list']
        try:
            if path in allowed_files:
                if env.get('REQUEST_METHOD') == 'GET':
                    return self.return_static_file(pathjoin(self.swift_dir,
                                                            path),
                                                   start_response, env)
                elif env.get('REQUEST_METHOD') == 'HEAD':
                    if path_prefix == 'ring' or path_prefix == 'ringbuilder':
                        return self.ring_or_builder_head(env, start_response)
                    else:
                        return self.http_not_found(start_response)
            elif path in allowed_paths:
                if not env.get('REQUEST_METHOD') == 'GET':
                    self._log_request(env, 400)
                    return self.http_bad_request(start_response, 'Try GET.')
                if path_prefix == 'ringbuilder':
                    return self.list_devices(path.split('/')[0],
                                             start_response, env)
                else:
                    self._log_request(env, 400)
                    return self.http_bad_request(start_response,
                                                 'Try /ringbuilder uri')
            else:
                self._log_request(env, 404)
                return self.http_not_found(start_response)
        except LockTimeout:
            self._log_request(env, 409)
            return self.http_conflict(start_response, 'Ring locked.')
        except Exception as err:
            self.logger.exception(_('error on ring get'))
            self._log_request(env, 500)
            return self.http_internal_server_error(start_response, str(err))

    def return_response(self, success, current_hash, content, start_response,
                        env):
        """ generate/return an http response to the client

        :params success: whether or not the requested succeeded
        :params current_hash: the current md5sum (if provide) to be returned
                             in the X-Current-Hash header.
        :params content: the content (if any) that should be returned in the
                         response body.
        :params start_response: start_response
        :returns: an http response
        """
        if success:
            self._log_request(env, 200)
            if content:
                self._log_request(env, 200)
                return self.http_ok(start_response, current_hash,
                                    json.dumps(content))
            else:
                self._log_request(env, 200)
                if isinstance(content, list):
                    return self.http_ok(start_response, current_hash, '[]')
                else:
                    return self.http_ok(start_response, current_hash)
        else:
            self._log_request(env, 400)
            return self.http_bad_request(start_response, str(content))

    @staticmethod
    def http_internal_server_error(start_response, content):
        """return a 500 error"""
        if not content.endswith('\r\n'):
            content += '\r\n'
        start_response('500 Internal Server Error',
                       [('Content-Length', str(len(content))),
                        ('Content-Type', 'text/plain')])
        return [content]

    @staticmethod
    def http_ok(start_response, ringhash=None, content=None):
        """return a 200 optionally setting the X-Current-Hash header and
        returning content"""
        if ringhash:
            if content:
                start_response('200 OK',
                               [('Content-Length', str(len(content))),
                                ('X-Current-Hash', str(ringhash)),
                                ('Content-Type', 'application/json')])
                return [content]
            else:
                start_response('200 OK', [('X-Current-Hash', str(ringhash))])
                return []
        else:
            start_response('200 OK', [('Content-Length', '0')])
            return []

    @staticmethod
    def http_bad_request(start_response, content):
        """return a 400 Bad request"""
        if not content.endswith('\r\n'):
            content += '\r\n'
        start_response('400 Bad Request',
                       [('Content-Length', str(len(content))),
                        ('Content-Type', 'text/plain')])
        return [content]

    @staticmethod
    def http_not_found(start_response, content='Resource could not be found.'):
        """return a 404 Not Found"""
        if not content.endswith('\r\n'):
            content += '\r\n'
        start_response('404 Not Found',
                       [('Content-Length', str(len(content))),
                        ('Content-Type', 'text/plain')])
        return [content]

    @staticmethod
    def http_conflict(start_response, content):
        """return a 409 Conflict"""
        if not content.endswith('\r\n'):
            content += '\r\n'
        start_response('409 Conflict',
                       [('Content-Length', str(len(content))),
                        ('Content-Type', 'text/plain')])
        return [content]

    @staticmethod
    def http_unauthorized(start_response):
        """return a 401 Unauthorized"""
        start_response('401 Unauthorized', [('Content-Length', '0')])
        return []

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            if req.path.startswith('/ringbuilder/'):
                if self.key and 'HTTP_X_RING_BUILDER_KEY' in env:
                    if env['HTTP_X_RING_BUILDER_KEY'] != self.key:
                        self._log_request(env, 401)
                        return self.http_unauthorized(start_response)
                    if req.method == 'GET' or req.method == 'HEAD':
                        return self.get_or_head(env, start_response)
                    elif req.method == 'POST':
                        return self.post(env, start_response, req.body)
                    else:
                        self._log_request(env, 400)
                        return self.http_bad_request(start_response,
                                                     'no such method')
                else:
                    self._log_request(env, 401)
                    return self.http_unauthorized(start_response)
            elif req.path.startswith('/ring/'):
                if self.key and 'HTTP_X_RING_BUILDER_KEY' in env:
                    if env['HTTP_X_RING_BUILDER_KEY'] != self.key:
                        self._log_request(env, 401)
                        return self.http_unauthorized(start_response)
                    if req.method == 'GET' or req.method == 'HEAD':
                        return self.get_or_head(env, start_response)
                    else:
                        self._log_request(env, 400)
                        return self.http_bad_request(start_response,
                                                     'no such method')
                else:
                    self._log_request(env, 401)
                    return self.http_unauthorized(start_response)
        except ValueError:
            self._log_request(env, 400)
            return self.http_bad_request(start_response, 'Bad Request')
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def ring_builder_filter(app):
        return RingBuilderMiddleware(app, conf)
    return ring_builder_filter
