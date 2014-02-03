"use strict";
var _ = require('lodash');
var fs = require('fs');
var git = require('git-node');
var path = require('path');
var Promise = require('mpromise');
var ObjectId = require('./ObjectId');
var obj_sort = require('./json/sort-object');
var JSON5 = require('./json/json-serialize');
var Collection = module.parent.exports;
var sift = require('sift');

Collection.prototype._init_store = function _init_store(__, ___, callback) {
    this.repo = git.repo(this._tdb._path + "/.git");
    var p = new Promise(callback);
    this._store = {};
    this.repo.setHead("master", function (err) {
        if (err) throw err;
        if (fs.existsSync(this._filename)) {
            fs.readFile(this._filename, function (err, data) {
                var temp = JSON.parse(data);
                this._store = JSON5.deserialize(temp);
                _.forEach(this._store, function (item) {
                    item._id = new ObjectId(item._id);
                });
                p.fulfill();
            }.bind(this));
        } else {
            p.fulfill();
        }
    }.bind(this));
    return p;
};


function persist(self, message) {
    var p = new Promise();
    var data = JSON.stringify(self._store, null, '\t');
    fs.writeFile(self._filename, data, function (err, written) {
        if (err) throw err;
        self._fsize = written;
        self.repo.saveAs("blob", data, function (err, hash) {
            if (err) throw err;
            var tree = {};
            tree[self._filename] = { mode: 33188, hash: hash };
            self.repo.saveAs("tree", tree, function (err, hash) {
                if (err) throw err;
                var commit = {
                    tree: hash,
                    parent: self.repo._parent,
                    author: { name: "someone", email: "someone@test.com" },
                    committer: { name: "someone", email: "someone@test.com" },
                    message: message
                };
                if (!self.repo._parent) delete commit.parent;
                self.repo.saveAs("commit", commit, function (err, hash) {
                    if (err) throw err;
                    self.repo._parent = hash;
                    self.repo.updateHead(hash, function (err) {
                        if (err) throw err;
                        p.fulfill();
                    });

                });
            });
        });
    });
    return p;
}


Collection.prototype._put = function (item) {
    var key = Collection.simplifyKey(item._id);
    var is_new = !(key in this._store);
    var obj = this._store[key] = obj_sort(item);
    var message = (is_new ? 'Inserting ' : 'Updating ') + item._id;
    return persist(this, message).then(function () { return obj; });
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
    return persist(this, 'Inserted ' + items.length).then(function () { return items; });
};


Collection.prototype._remove = function (item) {
    var key = Collection.simplifyKey(item._id);
    delete this._store[key];
    return persist(this, 'Removed ' + item._id).then(function () { return 1; });
};


Collection.prototype._remove_batch = function (items) {
    items.forEach(function (item) {
        var key = Collection.simplifyKey(item._id);
        delete this._store[key];
    }.bind(this));
    return persist(this, 'Removed ' + items.length).then(function () { return items.length; });
};


Collection.prototype._get = function _get_read(pos, cb) {
    var p = new Promise(cb);
    var obj = this._store[pos];
    p.fulfill(obj);
    return p;
};


Collection.prototype._stop = function (cb) {
    // this will prevent any tasks processed on this instance
    this._tq._stoped = true;
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


Collection.prototype._find = function (query, fields, skip, limit, sort, order) {
    if (query && '_id' in query && query._id === undefined) delete query._id;
    var work_set = _.values(this._store);
    if (!_.isEmpty(query)) work_set = sift(query, work_set);
    if (sort) work_set = _(work_set)
        .map(function (val) {return {key: sort in val ? val[sort] : null, val: val}; })
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
