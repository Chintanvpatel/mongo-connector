# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tails the oplog of a shard and returns entries
"""

import bson
import json
import logging
try:
    import Queue as queue
except ImportError:
    import queue
import sys
import time
import threading

import pymongo

from pymongo import CursorType
from bson.objectid import ObjectId
from bson.json_util import loads
from bson.json_util import dumps

from mongo_connector import errors, util
from mongo_connector.constants import DEFAULT_BATCH_SIZE
from mongo_connector.gridfs_file import GridFSFile
from mongo_connector.util import log_fatal_exceptions, retry_until_ok

LOG = logging.getLogger(__name__)


class DBThread(threading.Thread):
    """Thread that tails an oplog.

    Calls the appropriate method on DocManagers for each relevant oplog entry.
    """
    def __init__(self, primary_client, doc_managers,
                 mongos_client=None, **kwargs):
        super(DBThread, self).__init__()

        self.batch_size = kwargs.get('batch_size', DEFAULT_BATCH_SIZE)

        # The connection to the primary for this replicaSet.
        self.primary_client = primary_client

        # The connection to the mongos, if there is one.
        self.mongos_client = mongos_client

        # Are we allowed to perform a collection dump?
        self.collection_dump = kwargs.get('collection_dump', True)

        # The document manager for each target system.
        # These are the same for all threads.
        self.doc_managers = doc_managers

        # Boolean describing whether or not the thread is running.
        self.running = True

        # Stores the timestamp of the last oplog entry read.
        self.checkpoint = None

        # The set of namespaces to process from the mongo cluster.
        self.namespace_set = kwargs.get('ns_set', [])

        # The set of gridfs namespaces to process from the mongo cluster
        self.gridfs_set = kwargs.get('gridfs_set', [])

        # The dict of source namespaces to destination namespaces
        self.dest_mapping = kwargs.get('dest_mapping', {})

        # Whether the collection dump gracefully handles exceptions
        self.continue_on_error = kwargs.get('continue_on_error', False)

        # Database Name
        self.database_name = kwargs.get('database_name', 't')

        # Collection Name
        self.collection_name = kwargs.get('collection_name', 't')

        with open('applications.json') as app_config:    
            self.applications = json.load(app_config)

        # Set of fields to export
        self._exclude_fields = set([])
        self.fields = kwargs.get('fields', None)
        if kwargs.get('exclude_fields', None):
            self.exclude_fields = kwargs['exclude_fields']

        LOG.info('DBThread: Initializing oplog thread')

        self.db = self.primary_client[self.database_name][self.collection_name]
        if not self.db.find_one():
            err_msg = 'DBThread: No data for thread:'
            LOG.warning('%s' % (err_msg))
            self.running = False

    @property
    def fields(self):
        if self._fields:
            return list(self._fields)
        return None

    @property
    def exclude_fields(self):
        if self._exclude_fields:
            return list(self._exclude_fields)
        return None

    @fields.setter
    def fields(self, value):
        if self._exclude_fields:
            raise errors.InvalidConfiguration(
                "Cannot set 'fields' when 'exclude_fields' has already "
                "been set to non-empty list.")
        if value:
            self._fields = set(value)
            # Always include _id field
            self._fields.add('_id')
            self._projection = dict((field, 1) for field in self._fields)
        else:
            self._fields = set([])
            self._projection = None

    @exclude_fields.setter
    def exclude_fields(self, value):
        if self._fields:
            raise errors.InvalidConfiguration(
                "Cannot set 'exclude_fields' when 'fields' has already "
                "been set to non-empty list.")
        if value:
            self._exclude_fields = set(value)
            if '_id' in value:
                LOG.warning("OplogThread: Cannot exclude '_id' field, "
                            "ignoring")
                self._exclude_fields.remove('_id')
            if not self._exclude_fields:
                self._projection = None
            else:
                self._projection = dict(
                    (field, 0) for field in self._exclude_fields)
        else:
            self._exclude_fields = set([])
            self._projection = None

    @property
    def namespace_set(self):
        return self._namespace_set

    @namespace_set.setter
    def namespace_set(self, namespace_set):
        self._namespace_set = namespace_set
        self.update_oplog_ns_set()

    @property
    def gridfs_set(self):
        return self._gridfs_set

    @gridfs_set.setter
    def gridfs_set(self, gridfs_set):
        self._gridfs_set = gridfs_set
        self._gridfs_files_set = [ns + '.files' for ns in gridfs_set]
        self.update_oplog_ns_set()

    @property
    def gridfs_files_set(self):
        try:
            return self._gridfs_files_set
        except AttributeError:
            return []

    @property
    def oplog_ns_set(self):
        try:
            return self._oplog_ns_set
        except AttributeError:
            return []

    def update_oplog_ns_set(self):
        self._oplog_ns_set = []
        if self.namespace_set:
            self._oplog_ns_set.extend(self.namespace_set)
            self._oplog_ns_set.extend(self.gridfs_files_set)
            self._oplog_ns_set.extend(set(
                ns.split('.', 1)[0] + '.$cmd' for ns in self.namespace_set))
            self._oplog_ns_set.append("admin.$cmd")

    @log_fatal_exceptions
    def run(self):
        """Start the oplog worker.
        """
        LOG.debug("DBThread: Run thread started")
        while self.running is True:
            for app_id, config in self.applications.iteritems():
                LOG.debug("OplogThread: Getting cursor for %s" % (app_id))
                cursor = self.get_db_cursor(app_id, config)
                if (cursor.count() is not 0):
                    upsert_inc = 0
                    try:
                        LOG.debug("DBThread: about to process new oplog "
                                  "entries")
                        while cursor.alive and self.running:
                            LOG.debug("DBThread: Cursor is still"
                                      " alive and thread is still running.")
                            for n, entry in enumerate(cursor):

                                LOG.debug("DBThread: Iterating through cursor,"
                                          " document number in this cursor is %d"
                                          % n)
                                # Break out if this thread should stop
                                if not self.running:
                                    break

                                # Ignore the collection if it is not included
                                if self.oplog_ns_set and ns not in self.oplog_ns_set:
                                    LOG.debug("OplogThread: Skipping oplog entry: "
                                              "'%s' is not in the namespace set." %
                                              (ns,))
                                    continue

                                # use namespace mapping if one exists
                                ns = "%s.%s" % (self.database_name, self.collection_name)
                                timestamp = time.time()
                                for docman in self.doc_managers:
                                    self.set_index_name(app_id, docman)
                                    try:                            
                                        docman.upsert(entry, ns, timestamp)
                                        upsert_inc += 1

                                    except errors.OperationFailed:
                                        LOG.exception(
                                            "Unable to process oplog document %r"
                                            % entry)
                                    except errors.ConnectionFailed:
                                        LOG.exception(
                                            "Connection failed while processing oplog "
                                            "document %r" % entry)

                    except (pymongo.errors.AutoReconnect,
                            pymongo.errors.OperationFailure,
                            pymongo.errors.ConfigurationError):
                        LOG.exception(
                            "Cursor closed due to an exception. "
                            "Will attempt to reconnect.")

                    LOG.debug("DBThread: Documents "
                              "upserted: %d for application %s"
                              % (upsert_inc, app_id))
                    print("DBThread: Documents "
                              "upserted: %d for application %s"
                              % (upsert_inc, app_id))
            self.running = False

    def join(self):
        """Stop this thread from managing the oplog.
        """
        LOG.debug("DBThread: exiting due to join call.")
        self.running = False
        threading.Thread.join(self)

    def _pop_excluded_fields(self, doc):
        # Remove all the fields that were passed in exclude_fields.
        for field in self._exclude_fields:
            curr_doc = doc
            dots = field.split('.')
            remove_up_to = curr_doc
            end = dots[0]
            for part in dots:
                if not isinstance(curr_doc, dict) or part not in curr_doc:
                    break
                elif len(curr_doc) != 1:
                    remove_up_to = curr_doc
                    end = part
                curr_doc = curr_doc[part]
            else:
                remove_up_to.pop(end)
        return doc  # Need this to be similar to copy_included_fields.

    def _copy_included_fields(self, doc):
        # Copy over included fields to new doc
        new_doc = {}
        for field in self.fields:
            dots = field.split('.')
            curr_doc = doc
            for part in dots:
                if part not in curr_doc:
                    break
                else:
                    curr_doc = curr_doc[part]
            else:
                # If we found the field in the original document, copy it
                edit_doc = new_doc
                for part in dots[:-1]:
                    edit_doc = edit_doc.setdefault(part, {})
                edit_doc[dots[-1]] = curr_doc

        return new_doc

    def filter_oplog_entry(self, entry):
        """Remove fields from an oplog entry that should not be replicated.

        NOTE: this does not support array indexing, for example 'a.b.2'"""
        if not self._fields and not self._exclude_fields:
            return entry
        elif self._fields:
            filter_fields = self._copy_included_fields
        else:
            filter_fields = self._pop_excluded_fields

        entry_o = entry['o']
        # 'i' indicates an insert. 'o' field is the doc to be inserted.
        if entry['op'] == 'i':
            entry['o'] = filter_fields(entry_o)
        # 'u' indicates an update. The 'o' field describes an update spec
        # if '$set' or '$unset' are present.
        elif entry['op'] == 'u' and ('$set' in entry_o or '$unset' in entry_o):
            if '$set' in entry_o:
                entry['o']["$set"] = filter_fields(entry_o["$set"])
            if '$unset' in entry_o:
                entry['o']["$unset"] = filter_fields(entry_o["$unset"])
            # not allowed to have empty $set/$unset, so remove if empty
            if "$set" in entry_o and not entry_o['$set']:
                entry_o.pop("$set")
            if "$unset" in entry_o and not entry_o['$unset']:
                entry_o.pop("$unset")
            if not entry_o:
                return None
        # 'u' indicates an update. The 'o' field is the replacement document
        # if no '$set' or '$unset' are present.
        elif entry['op'] == 'u':
            entry['o'] = filter_fields(entry_o)

        return entry

    def filter_by_app(self, doc, docman):
        if 'app_id' in doc and str(doc['app_id']) in self.applications:
            docman.set_index(self.applications[str(doc['app_id'])]['index'])
            return doc, docman
        else:
            return None, docman

    def get_db_cursor(self, app_id, config):
        # Get cursor to iterate
        query = config['query'] or {}
        query = loads(dumps(query))
        cursor = self.db.find(query)
        #docman.set_index(config['index'])
        return cursor

    def set_index_name(self, app_id, docman):
        docman.set_index(self.applications[app_id]['index'])
        return docman

    def _get_oplog_timestamp(self, newest_entry):
        """Return the timestamp of the latest or earliest entry in the oplog.
        """
        sort_order = pymongo.DESCENDING if newest_entry else pymongo.ASCENDING
        curr = self.oplog.find({'op': {'$ne': 'n'}}).sort(
            '$natural', sort_order
        ).limit(-1)

        try:
            ts = next(curr)['ts']
        except StopIteration:
            LOG.debug("OplogThread: oplog is empty.")
            return None

        LOG.debug("OplogThread: %s oplog entry has timestamp %d."
                  % ('Newest' if newest_entry else 'Oldest', ts.time))
        return ts

    def get_oldest_oplog_timestamp(self):
        """Return the timestamp of the oldest entry in the oplog.
        """
        return self._get_oplog_timestamp(False)

    def get_last_oplog_timestamp(self):
        """Return the timestamp of the newest entry in the oplog.
        """
        return self._get_oplog_timestamp(True)

    def _cursor_empty(self, cursor):
        try:
            next(cursor.clone().limit(-1))
            return False
        except StopIteration:
            return True
