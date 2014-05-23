"use strict";
var _ = require('lodash-contrib');
var fs = require('fs');
var git = require('git-node');
var path = require('path');
var Promise = require('mpromise');
var ObjectId = require('./ObjectId');
var obj_sort = require('./json/sort-object');
var JSON5 = require('./json/json-serialize');
var Collection = module.parent.exports;
var sift = require('sift');

var AUTHOR = { name: "someone", email: "someone@test.com" };
var COMMIT_TEMPLATE = { author: AUTHOR, committer: AUTHOR };

Collection.prototype._init_store = function _init_store() {
    var self = this;
    if (!fs.existsSync(self._filename)) {
        self._store = {};
        return Promise.fulfilled();
    }
    var pi = new Promise;
    fs.readFile(self._filename, function (err, data) {
        if (err) throw err;
        var temp = JSON.parse(data);
        self._store = JSON5.deserialize(temp) || {};
        _.forEach(self._store, function (item) {
            var raw_id = item._id;
            if (raw_id.toString().length == 24 || _.has(raw_id, '$oid'))
                item._id = ObjectId.tryToParse(raw_id);
        });
        pi.fulfill();
    });
    return pi;
};


//Collection.prototype.persist_simple = function () {
//    var data = JSON.stringify(this._store, null, '\t');
//    this._fsize = fs.writeFileSync(this._filename, data);
//    return Promise.fulfilled();
//};

Collection.prototype.persist = function (message) {
    var self = this;
    var gitDir = path.join(this._tdb._path, '.git');
    if (!fs.existsSync(this._tdb._path)) fs.mkdirSync(this._tdb._path);
    var repo = this.repo = git.repo(gitDir);
    var p = Promise.fulfilled().then(
        function () {
            var pi = new Promise;
            repo.getHead(
                function (err, head) {
                    if (!head || err) {
                        repo.setHead("master", function (err) {
                            if (err) throw err;
                            pi.fulfill();
                        });
                    } else {
                        self.repo.resolveHashish("HEAD", function (err, headCommit) {
                            if (err) {
                                headCommit = null;
                            }
                            pi.fulfill(headCommit);
                        });
                    }
                }
            );
            return pi;
        }
    ).then(
        function (headCommit) {
            var pi = new Promise();
            var data = JSON.stringify(self._store, null, '\t');
            self._fsize = fs.writeFileSync(self._filename, data);
            self.repo.saveAs("blob", data, function (err, blobHash) {
                if (err) {
                    throw err;
                }
                var tree = {};
                tree[self._filename] = { mode: 33188, hash: blobHash };
                self.repo.saveAs("tree", tree, function (err, treeHash) {
                    if (err) {
                        throw err;
                    }
                    var commit = _.assign({ tree: treeHash, parent: headCommit, message: message }, COMMIT_TEMPLATE);
                    self.repo.saveAs("commit", commit, function (err, commitHash) {
                        if (err) {
                            throw err;
                        }
                        self.repo.updateHead(commitHash, function (err) {
                            if (err) {
                                throw err;
                            }
                            pi.fulfill();
                        });
                    });
                });
            });
            return pi;
        }
    );
    return p;
};


Collection.prototype._put = function (item) {
    var key = Collection.simplifyKey(item._id);
    var is_new = !(key in this._store);
    var obj = this._store[key] = obj_sort(item);
    var message = (is_new ? 'Inserting ' : 'Updating ') + item._id;
    var p = Promise
        .fulfilled()
        .then(this.persist.bind(this, message))
        .then(function () {
            return obj;
        }
    );
    return p.end();
};


Collection.prototype._put_batch = function (items) {
    var self = this;
    _(items).forEach(function (item) {
        if (_.isUndefined(item._id)) {
            item._id = new self._tdb.ObjectID();
        }
        var key = Collection.simplifyKey(item._id);
        self._store[key] = obj_sort(item);
    });
    var p = Promise
        .fulfilled()
        .then(
            this.persist.bind(this, 'Inserted ' + items.length))
        .then(
        function () {
            return items;
        }
    );
    return p.end();
};


Collection.prototype._remove_batch = function (items) {
    if (!items) {
        this._store = {};
        if (fs.existsSync(this._filename)) fs.unlinkSync(this._filename);
        return Promise.fulfilled();
    }
    var self = this;
    items.forEach(function (item) {
        var key = Collection.simplifyKey(item._id);
        delete self._store[key];
    });
    var p = Promise.fulfilled().then(
        function () {
            return self.persist('Removed ' + items.length);
        }
    ).then(
        function () {
            return items.length;
        }
    ).end();
    return p;
};


Collection.prototype._stop = function (cb) {
    cb(null, false);
};


Collection.prototype._rename = function _rename(nname, opts, cb) {
    fs.rename(path.join(this._tdb._path, this._name), path.join(this._tdb._path, nname), function (err) {
        if (err) throw err;
        delete this._tdb._cols[this._name];
        this._tdb._cols[nname] = this;
        this.collectionName = this._name = nname;
        cb();
    }.bind(this));
};


Collection.prototype._drop = function (cb) {
    if (!fs.existsSync(this._filename)) {
        cb();
        return;
    }
    fs.unlink(this._filename, cb);
};


function handleKeys(query, store) {

    if (!query || !('_id' in query)) {
        return _.values(store);
    }

    var wanted_id = query._id;
    delete query._id;

    if (_.isNull(wanted_id) || _.isUndefined(wanted_id))
        return [];

    if (!_.isPlainObject(wanted_id)) {
        var key = wanted_id.toHexString ? wanted_id.toHexString() : JSON.stringify(wanted_id).replace(/"/g, '');
        var val = store[key];
        return _.compact([val]);
    }

    _.walk.preorder(wanted_id, function (value, key, parent) {
        if (value instanceof ObjectId) parent[key] = value.toString();
    });
    var keys = sift(wanted_id, _.keys(store));
    return _(store).pick(keys).values().valueOf();
}


Collection.prototype._find = function (rawquery, fields, skip, limit, sort, order) {
    var query = this._tdb._cloneDeep(rawquery);
    var work_set = handleKeys(query, this._store);
    if (!_.isEmpty(query)) work_set = sift(query, work_set);
    if (sort) work_set = _(work_set)
        .map(function (val) {
            return {key: (val && sort in val) ? val[sort] : null, val: val};
        })
        .sortBy('key')
        .pluck('val')
        .valueOf();
    if (order === -1) work_set = work_set.reverse();
    skip = skip || 0;
    limit = limit || undefined;
    if (skip || limit) work_set = work_set.slice(skip, limit);
    var res = _.cloneDeep(work_set, function (value) {
        if (value instanceof ObjectId) return value;
    });
    return res;
};


Collection.prototype.ensureIndex = function () {
    var cb = arguments[arguments.length-1];
    if (_.isFunction(cb)) cb();
};
