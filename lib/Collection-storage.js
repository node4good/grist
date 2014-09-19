"use strict";
var _ = require('lodash-contrib');
var fs = require('fs');
var path = require('path');
var MPromise = require('mpromise');
var ObjectId = require('./ObjectId');
var obj_sort = require('./json/sort-object');
var JSONS = require('./json/json-special');
var Collection = module.parent.exports;
var sift = require('sift');


/* git-node */
var git_node_fs = require('git-node-fs/mixins/fs-db');

function createRepo(dbDir) {
    var repo = {};
    git_node_fs(repo, path.join(dbDir, '.git'));

    var d = MPromise.deferred();
    repo.readRef("HEAD", function initReadRef(err, head) {
        if (err) return d.reject(err);
        if (head) return d.resolve(repo);
        repo.init("refs/heads/master", function (err) {
            if (err) return d.reject(err);
            d.resolve(repo);
        });
    });
    return d.promise;
}

function makeCommit(treeHash, head, msg) {
    var author = {name: "someone", email: "someone@test.com", date: new Date()};
    var commit = {tree: treeHash, parents: [head], message: msg, author: author, committer: author};
    return commit;
}
/* end git-node  */



Collection.prototype._init_store = function _init_store() {
    var self = this;
    if (!fs.existsSync(self._filename)) {
        self._store = {};
        return;
    }
    var data = fs.readFileSync(self._filename);
    var temp = JSON.parse(data);
    self._store = JSONS.deserialize(temp);
};


Collection.prototype._persist = function (msg) {
    var self = this;
    var p = createRepo(this._tdb._path).then(
        function _persist(repo) {
            var pi = new MPromise();
            var data = JSON.stringify(self._store, null, '\t');
            var bdata = new Buffer(data);
            fs.writeFile(self._filename, bdata, function fsWriteCB(err, _size) {
                self._fsize = _size;
                repo.saveAs("blob", bdata, function repoSaveBlobCB(err, blobHash) {
                    if (err) {
                        throw err;
                    }
                    var tree = {};
                    tree[self._filename] = {mode: 33188, hash: blobHash};
                    repo.saveAs("tree", tree, function repoSaveTreeCB(err, treeHash) {
                        if (err) {
                            throw err;
                        }
                        repo.readRef("HEAD", function (err, head) {
                            if (err) {
                                throw err;
                            }
                            var commit = makeCommit(treeHash, head, msg);
                            repo.saveAs("commit", commit, function repoCommitCB(err, commitHash) {
                                if (err) {
                                    throw err;
                                }
                                pi.fulfill();
                            });
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
    var p = MPromise
        .fulfilled()
        .then(this._persist.bind(this, message))
        .then(function () {
            return obj;
        }
    );
    return p.end();
};


Collection.prototype._put_batch = function (items) {
    var self = this;
    _(items).forEach(function (itm) {
        if (_.isUndefined(itm._id)) {
            itm._id = new self._tdb.ObjectID();
        }
        var key = Collection.simplifyKey(itm._id);
        self._store[key] = obj_sort(itm);
    });
    var p = MPromise.fulfilled()
        .then(this._persist.bind(this, 'Inserted ' + items.length))
        .then(function () {
            return items;
        });
    return p;
};


Collection.prototype._remove_batch = function (items) {
    if (!items) {
        this._store = {};
        if (fs.existsSync(this._filename)) fs.unlinkSync(this._filename);
        return MPromise.fulfilled();
    }
    var self = this;
    items.forEach(function (item) {
        var key = Collection.simplifyKey(item._id);
        delete self._store[key];
    });
    var p = MPromise.fulfilled().then(
        function () {
            return self._persist('Removed ' + items.length);
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


Collection.prototype._drop = function () {
    if (fs.existsSync(this._filename)) {
        fs.unlinkSync(this._filename);
    }
    this._store = {};
    this._tq = null;
    this._idx = {};
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
    var cb = arguments[arguments.length - 1];
    if (_.isFunction(cb)) cb();
};
