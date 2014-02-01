'use strict';
var path = require('path');

var _ = require('lodash');
var safe = require('safe');
var async = require('async');
var Promise = require('mpromise');

var Cursor = require('./Cursor');
var wqueue = require('./wqueue');
var tindex = require('./tindex');
var Cache = require("./Cache");
var Updater = require('./updater');


function Collection() {
    this._tdb = null;
    this._name = null;
    this._store = {};
    this._fd = null;
    this._fsize = null;
    this._id = 1;
    this._tq = null;
    this._idx = {};
    this._cache = null;
    // native mongo db compatibility attrs
    this.collectionName = null;
}
module.exports = Collection;


Collection.simplifyKey = function simplifyKey(key) {
    if (_.isNumber(key)) return key;
    return key.toJSON ? key.toJSON() : String(key);
};


Collection.prototype.init = function (tdb, name, options, callback) {
    var self = this;
    this._tdb = tdb;
    this._cache = new Cache(tdb, tdb._gopts.cacheSize);
    this._cmaxobj = tdb._gopts.cacheMaxObjSize || 1024;
    this._name = this.collectionName = name;
    this._filename = path.join(this._tdb._path, this._name + '.json');
    this._tq = new wqueue(100);

    var p = new Promise(callback);
    self._init_store(0, 0).then(
        function () {
            var ip = Promise.fulfilled();
            var values = _.values(self._store);
            values.forEach(
                function (rec) {
                    ip = ip.then(
                        function () {
                            return self._get(rec.pos);
                        }
                    ).then(
                        function (obj) {
                            var id = Collection.simplifyKey(obj._id);
                            _(self._idx).forEach(function (v) { v.set(obj, id); });
                        }
                    );
                }
            );
            ip = ip.then(function () {
                return self.ensureIndex({_id: 1}, {unique: true});
            });
            ip = ip.then(function () {
                p.fulfill();
            }).end();
        }
    );
    return p;
};


Collection.prototype.drop = function (cb) {
    this._tdb.dropCollection(this._name, cb);
};


Collection.prototype.rename = function rename(nname, opts, cb) {
    var self = this;
    var err = self._tdb._nameCheck(nname);
    if (err) throw err;
    this._rename(nname, opts, cb);
};


Collection.prototype.createIndex = Collection.prototype.ensureIndex = function (obj, options, cb) {
    var self = this;
    if (_.isFunction(options) && !cb) {
        cb = options;
        options = {};
    }
    cb = cb || function () {};
    options = options || {};

    var p = new Promise(cb);
    var c = new Cursor(this, {}, {}, {});
    c.sort(obj);
    if (c._err) {
        p.reject(c._err);
        return p;
    }
    var key = c._sort;

    if (!key) throw new Error("No fields are specified");

    var index = self._idx[key];
    if (index) {
        p.fulfill(index.name);
        return p;
    }

    // force array support when global option is set
    if (_.isUndefined(options._tiarr) && self._tdb._gopts.searchInArray)
        options._tiarr = true;

    var index_name = key + "_" + (key == '_id' ? '' : c._order);
    index = new tindex(key, self, options, index_name);
    // otherwise register index operation
    var p_ret = _(self._store).reduce(
        function (seed, record) {
            return seed.then(
                function () {
                    return self._get(record.pos);
                }
            ).then(
                function (obj) {
                    index.set(obj, Collection.simplifyKey(obj._id));
                }
            );
        },
        Promise.fulfilled()
    ).then(
        function () {
            self._idx[key] = index;
            return index.name;
        }
    ).onResolve(cb);
    return p_ret;
}
;


Collection.prototype.indexExists = function (idx, cb) {
    if (!_.isArray(idx))
        idx = [idx];
    var i = _.intersection(idx, _(this._idx).values().map('name').value());
    cb(null, i.length == idx.length);
};


Collection.prototype.indexes = function (cb) {
    var self = this;
    this._tq.add(function (cb) {
        cb(null, _.values(self._idx));
    }, false, cb);
};


Collection.prototype.insert = function (docs, unused, callback) {
    var self = this;
    if (_.isFunction(unused) && arguments.length == 2) {
        callback = unused;
    }
    if (!_.isArray(docs))
        docs = [docs];

    var p = new Promise(callback);
    async.mapSeries(
        docs,
        function (doc, cb) {
            if (_.isUndefined(doc._id)) {
                doc._id = new self._tdb.ObjectID();
            }
            self._put(doc, false, cb);
        },
        function (err, newDocs) {
            p.resolve(err, newDocs);
        }
    );
    return p;
};

Collection.prototype._wrapTypes = function (obj) {
    var self = this;
    _.each(obj, function (v, k) {
        if (_.isDate(v))
            obj[k] = {$wrap: "$date", v: v.valueOf(), h: v};
        else if (v instanceof self._tdb.ObjectID)
            obj[k] = {$wrap: "$oid", v: v.toJSON()};
        else if (v instanceof self._tdb.Binary)
            obj[k] = {$wrap: "$bin", v: v.toJSON()};
        else if (_.isObject(v))
            self._wrapTypes(v);

    });
    return obj;
};

Collection.prototype._ensureIds = function (obj) {
    var self = this;
    _.each(obj, function (v, k) {
        if (k.length > 0) {
            if (k[0] == '$')
                throw new Error("key " + k + " must not start with '$'");

            if (k.indexOf('.') != -1)
                throw new Error("key " + k + " must not contain '.'");
        }
        if (_.isObject(v)) {
            if (v instanceof self._tdb.ObjectID) {
                if (v.id < 0) {
                    v._persist(++self._id);
                }
            }
            else
                self._ensureIds(v);
        }
    });
    return obj;
};


Collection.prototype._unwrapTypes = function (obj) {
    var self = this;
    _.each(obj, function (v, k) {
        if (_.isObject(v)) {
            switch (v.$wrap) {
                case "$date":
                    obj[k] = new Date(v.v);
                    break;
                case "$oid":
                    var oid = new self._tdb.ObjectID(v.v);
                    obj[k] = oid;
                    break;
                case "$bin":
                    var bin = new self._tdb.Binary(new Buffer(v.v, 'base64'));
                    obj[k] = bin;
                    break;
                default:
                    self._unwrapTypes(v);
            }
        }
    });
    return obj;
};


Collection.prototype.count = function (query, options, cb) {
    if (arguments.length == 1) {
        cb = arguments[0];
        options = null;
        query = null;
    }
    if (arguments.length == 2) {
        query = arguments[0];
        cb = arguments[1];
        options = null;
    }
    var p = new Promise(cb);
    if (query && !_.isEmpty(query)) {
        return this.find(query, options, function (err, docs) {
            p.resolve(err, docs.length);
        });
    }
    p.fulfill(_.size(this._store));
    return p;

};

Collection.prototype.stats = function (cb) {
    var self = this;
    this._tq.add(function (cb) {
        cb(null, {count: _.size(self._store)});
    }, false, cb);
};


var findOpts = ['limit', 'sort', 'fields', 'skip', 'hint', 'timeout', 'batchSize', 'safe', 'w'];

Collection.prototype.findOne = function () {
    var findArgs = _.toArray(arguments);
    var cb = findArgs.pop();
    var p = new Promise(cb);
    this.find.apply(this, findArgs).limit(1).nextObject(p.resolve.bind(p));
    return p;
};


function argsForFind(args) {
    var opts = {};
    if (args.length === 0) return opts;
    // guess callback, it is always latest
    var cb = _.last(args);
    if (_.isFunction(cb)) {
        args.pop();
        opts.cb = cb;
    }
    opts.query = args.shift();
    if (args.length === 0) return opts;
    if (args.length == 1) {
        var val = args.shift();
        // if val looks like findOpt
        if (_.intersection(_.keys(val), findOpts).length) {
            opts = _.merge(opts, val);
        } else {
            opts.fields = val;
        }
        return opts;
    }
    opts.fields = args.shift();
    if (args.length == 1) {
        opts = _.merge(opts, args.shift());
    } else {
        opts.skip = args.shift();
        opts.limit = args.shift();
    }
    return opts;
}


Collection.prototype.find = function () {
    var opts = argsForFind(_.toArray(arguments));
    var cursor = new Cursor(this, opts.query, opts.fields, opts);
    if (opts.skip) cursor.skip(opts.skip);
    if (opts.limit) cursor.limit(opts.limit);
    if (opts.sort) cursor.sort(opts.sort);
    if (opts.cb)
        return opts.cb(null, cursor);
    else
        return cursor;
};


Collection.prototype.update = function (query, doc, opts, cb) {
    var self = this;
    if (_.isFunction(opts) && !cb) {
        cb = opts;
    }
    opts = opts || {};
    if (opts.w > 0 && !_.isFunction(cb))
        throw new Error("Callback is required for safe update");
    cb = cb || function () {};
    if (!_.isObject(query))
        throw new Error("selector must be a valid JavaScript object");
    if (!_.isObject(doc))
        throw new Error("document must be a valid JavaScript object");

    var multi = opts.multi || false;
    var updater = new Updater(doc, self._tdb);
    var $doc = updater.hasAtomic() ? null : doc;
    var p = new Promise(cb);
    self._find(query, null, 0, multi ? null : 1).then(function (res) {
        if (_.isEmpty(res)) {
            if (!opts.upsert) return cb(null, 0);
            $doc = $doc || query;
            $doc = self._tdb._cloneDeep($doc);
            updater.update($doc, true);
            if (_.isUndefined($doc._id))
                $doc._id = new self._tdb.ObjectID();

            self._put($doc, false).then(function () {
                p.fulfill(1, {updatedExisting: false, upserted: $doc._id, n: 1});
            });
            return;
        }
        var pr = Promise.fulfilled();
        res.forEach(function (obj) {
            var udoc = $doc;
            if (!$doc) {
                udoc = obj;
                updater.update(udoc);
            }
            udoc._id = obj._id;
            pr = pr.then(function () {self._put(udoc, false);});
        });
        pr.then(
            function () {
                p.fulfill(res.length, {updatedExisting: true, n: res.length})
            }
        );
    });
    return p;
};


Collection.prototype.findAndModify = function (query, sort, doc, opts, cb) {
    var self = this;
    if (_.isFunction(opts) && !cb) {
        cb = opts;
        opts = {};
    }
    var updater = new Updater(doc, self._tdb);
    var $doc = updater.hasAtomic() ? null : doc;

    var c = new Cursor(this, {}, opts.fields || {}, {});
    c.sort(sort);
    if (c._err)
        return safe.back(cb, c._err);

    self._find(query, null, 0, 1, c._sort, c._order, safe.sure(cb, function (res) {
        if (_.isEmpty(res)) {
            if (!opts.upsert) return cb();
            $doc = $doc || query;
            $doc = self._tdb._cloneDeep($doc);
            updater.update($doc, true);
            if (_.isUndefined($doc._id))
                $doc._id = new self._tdb.ObjectID();
            self._put($doc, false, safe.sure(cb, function () {
                cb(null, opts.new ? c._projectFields($doc) : {});
            }));
        } else {
            self._get(res[0], safe.sure(cb, function (obj) {
                var robj = (opts.new && !opts.remove) ? obj : self._tdb._cloneDeep(obj);
                // remove current version of doc from indexes
                _(self._idx).forEach(function (v) {
                    v.del(obj, Collection.simplifyKey(obj._id));
                });
                var udoc = $doc;
                if (!$doc) {
                    udoc = obj;
                    updater.update(udoc);
                }
                udoc._id = obj._id;
                // put will add it back to indexes
                self._put(udoc, opts.remove ? true : false, safe.sure(cb, function () {
                    cb(null, c._projectFields(robj));
                }));
            }));
        }
    }));
};


Collection.prototype.save = function (doc, __, callback) {
    var self = this;
    var args = _.toArray(arguments);
    callback = args.pop();
    callback = _.isFunction(callback) ? callback : null;
    doc = doc || {};
    return Promise.fulfilled().then(
        function () {
            if (_.isUndefined(doc._id)) {
                doc._id = new self._tdb.ObjectID();
                return;
            }
            var id = Collection.simplifyKey(doc._id);
            var pos = self._store[id];
            // check if document is new
            if (!pos) {
                return;
            }
            // if so we need to fetch it to update index
            return self._get(pos.pos).then(function (oldDoc) {
                // remove current version of doc from indexes
                _(self._idx).forEach(function (v) {
                    v.del(oldDoc, id);
                });
            });
        }
    ).then(
        function () {
            var def = Promise.deferred();
            self._put(doc, false, def.callback);
            return def.promise;
        }
    ).onResolve(callback);
};


Collection.prototype.remove = function (query, opts, callback) {
    var self = this;
    if (_.isFunction(query)) {
        callback = query;
        query = opts = {};
    } else if (_.isFunction(opts)) {
        callback = opts;
        opts = {};
    }
    opts = opts || {};
    if (opts.w > 0 && !_.isFunction(callback))
        throw new Error("Callback is required");
    callback = callback || function () {};
    var single = opts.single || false;
    var limit = single ? 1 : null;
    var p = new Promise(callback);
    self._find(query, null, 0, limit).then(function (res) {
        self._remove_batch(res, p.resolve.bind(p));
    });
    return p;
};

Collection.prototype.findAndRemove = function (query, sort, opts, cb) {
    var self = this;

    if (_.isFunction(sort) && !cb && !opts) {
        cb = sort;
        sort = {};
        opts = {};
    } else if (_.isFunction(opts) && !cb) {
        cb = opts;
        opts = {};
    }

    var c = new Cursor(this, {}, {}, {});
    c.sort(sort);
    if (c._err)
        return safe.back(cb, c._err);

    self._find(query, null, 0, 1, c._sort, c._order, safe.sure(cb, function (res) {
        if (res.length === 0)
            return cb();
        self._get(res[0], safe.sure(cb, function (obj) {
            self._remove(obj, safe.sure(cb, function () {
                cb(null, obj);
            }));
        }));
    }));
};


Collection.prototype._stop_and_drop = function (cb) {
    this._stop();
    this._drop(cb);
};


require('./Collection-ext');
require('./Collection-storage');
