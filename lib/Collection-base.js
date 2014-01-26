var path = require('path');
var crypto = require('crypto');

var _ = require('lodash');
var safe = require('safe');
var async = require('async');
var Promise = require('mpromise');

var Cursor = require('./Cursor');
var wqueue = require('./wqueue');
var tindex = require('./tindex');
var Cache = require("./Cache");
var tutils = require('./utils');
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

Collection.prototype.init = function (tdb, name, options, callback) {
    var self = this;
    this._tdb = tdb;
    this._cache = new Cache(tdb, tdb._gopts.cacheSize);
    this._cmaxobj = tdb._gopts.cacheMaxObjSize || 1024;
    this._name = this.collectionName = name;
    this._filename = path.join(this._tdb._path, this._name);
    this._tq = new wqueue(100);

    var p = new Promise;
    p.onResolve(function () {
        if (callback) callback.apply(null, arguments);
    });
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
                            var id = simplifyKey(obj._id);
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
            });
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
                    index.set(obj, simplifyKey(obj._id));
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


Collection.prototype._put = function (item, remove, cb) {
    var self = this;
    try {
        item = self._ensureIds(item);
    } catch (err) {
        err.errmsg = err.toString();
        throw err;
    }
    if (_.isUndefined(item._id))
        return cb(new Error("Invalid object key (_id)"));
    item = self._wrapTypes(item);
    var sobj = new Buffer(remove ? "" : JSON.stringify(item));
    item = self._unwrapTypes(item);
    var key = {_id: simplifyKey(item._id), _uid: self._id, _dt: (new Date()).valueOf()};
    if (remove) {
        key._a = "del";
    } else {
        var hash = crypto.createHash('md5');
        hash.update(sobj, 'utf8');
        key._s = hash.digest('hex');
    }
    var skey = new Buffer(JSON.stringify(key));
    var zeros = "0000000000";
    var lobj = sobj.length.toString();
    var lkey = skey.length.toString();
    lobj = zeros.substr(0, zeros.length - lobj.length) + lobj;
    lkey = zeros.substr(0, zeros.length - lkey.length) + lkey;
    var h1 = {k: lkey, o: lobj, v: "001"};
    var buf = new Buffer(JSON.stringify(h1) + "\n" + skey + "\n" + sobj + "\n");

    // check index update
    if (item && !remove) {
        try {
            _(self._idx).forEach(function (v) {
                v.set(item, key._id, true);
            });
        } catch (err) {
            err.errmsg = err.toString();
            throw err;
        }
    }

    this._put_write(sobj, key, item, buf, remove, function (err) {
        if (err) throw err;
        // update index
        var method_name = remove ? 'del' : 'set';
        _(self._idx).forEach(function (v) { v[method_name](item, key._id); });
        cb(null, item);
    });
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
    if (!(!query || _.isEmpty(query))) {
        return this.find(query, options);
    }
    var p = new Promise(cb);
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
    var findArgs = Array.prototype.slice.call(arguments, 0, arguments.length - 1);
    var cb = arguments[arguments.length - 1];
    this.find.apply(this, findArgs).limit(1).nextObject(cb);
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


function simplifyKey(key) {
    if (_.isNumber(key)) return key;
    return key.toJSON ? key.toJSON() : String(key);
}


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
    this._tq.add(function (cb) {
        self.__find(query, null, 0, multi ? null : 1, null, null, opts.hint, {}, safe.sure(cb, function (res) {
            if (_.isEmpty(res)) {
                if (!opts.upsert) return cb(null, 0);
                $doc = $doc || query;
                $doc = self._tdb._cloneDeep($doc);
                updater.update($doc, true);
                if (_.isUndefined($doc._id))
                    $doc._id = new self._tdb.ObjectID();
                self._put($doc, false, safe.sure(cb, function () {
                    cb(null, 1, {updatedExisting: false, upserted: $doc._id, n: 1});
                }));
            } else {
                async.forEachSeries(res, function (pos, cb) {
                    self._get(pos, safe.sure(cb, function (obj) {
                        // remove current version of doc from indexes
                        _(self._idx).forEach(function (v) {
                            v.del(obj, simplifyKey(obj._id));
                        });
                        var udoc = $doc;
                        if (!$doc) {
                            udoc = obj;
                            updater.update(udoc);
                        }
                        udoc._id = obj._id;
                        // put will add it back to indexes
                        self._put(udoc, false, cb);
                    }));
                }, safe.sure(cb, function () {
                    cb(null, res.length, {updatedExisting: true, n: res.length});
                }));
            }
        }));
    }, true, cb);
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

    this._tq.add(function (cb) {
        self.__find(query, null, 0, 1, c._sort, c._order, opts.hint, {}, safe.sure(cb, function (res) {
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
                        v.del(obj, simplifyKey(obj._id));
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
    }, true, cb);
};


Collection.prototype.save = function (doc, opts, cb) {
    var self = this;
    cb = _.isFunction(doc) ? doc : _.isFunction(opts) ? opts : cb;
    cb = cb || function () {};
    doc = doc || {};
    this._tq.add(function (cb) {
        var res = doc;
        (function (cb) {
            if (_.isUndefined(doc._id)) {
                doc._id = new self._tdb.ObjectID();
                cb();
                return;
            }
            var id = simplifyKey(doc._id);
            var pos = self._store[id];
            // check if document with this id already exist
            if (!pos) {
                cb();
                return;
            }
            // if so we need to fetch it to update index
            self._get(pos.pos, safe.sure(cb, function (oldDoc) {
                // remove current version of doc from indexes
                _(self._idx).forEach(function (v) {
                    v.del(oldDoc, id);
                });
                res = 1;
                cb();
            }));
        })(safe.sure(cb, function () {
                self._put(doc, false, safe.sure(cb, function () {
                    cb(null, res); // when update return 1 when new save return obj
                }));
            }));
    }, true, cb);
};


Collection.prototype.remove = function (query, opts, cb) {
    var self = this;
    if (_.isFunction(query)) {
        cb = query;
        query = opts = {};
    } else if (_.isFunction(opts)) {
        cb = opts;
        opts = {};
    }
    opts = opts || {};
    if (opts.w > 0 && !_.isFunction(cb))
        throw new Error("Callback is required");
    cb = cb || function () {};
    var single = opts.single || false;
    this._tq.add(function (cb) {
        self.__find(query, null, 0, single ? 1 : null, null, null, opts.hint, {}, safe.sure(cb, function (res) {
            async.forEachSeries(res, function (pos, cb) {
                self._get(pos, safe.sure(cb, function (obj) {
                    self._put(obj, true, cb);
                }));
            }, safe.sure(cb, function () {
                cb(null, res.length);
            }));
        }));
    }, true, cb);
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

    this._tq.add(function (cb) {
        self.__find(query, null, 0, 1, c._sort, c._order, opts.hint, {}, safe.sure(cb, function (res) {
            if (res.length === 0)
                return cb();
            self._get(res[0], safe.sure(cb, function (obj) {
                self._put(obj, true, safe.sure(cb, function () {
                    cb(null, obj);
                }));
            }));
        }));
    }, true, cb);
};


Collection.prototype.__find = function (query, fields, skip, limit, sort_, order, hint, arFields, cb) {
    var sort = sort_;
    var self = this;
    var res = [];
    var range = [];
    // now simple non-index search
    var found = 0;
    var qt = self._tdb.Finder.matcher(query);
    var pi = [];
    var io = {};
    // for non empty query check indexes that we can use
    if (_.size(qt) > 0) {
        _(self._idx).forEach(function (v, k) {
            if (qt._ex(k) == 1 && (!hint || hint[k]))
                pi.push(k);
        });
    }
    // if possible indexes found split the query and process
    // indexes separately
    if (pi.length > 0) {
        _(pi).forEach(function (v) {
            io[v] = qt.split(v);
        });
        var p = [];
        _(io).forEach(function (v, k) {
            var r = v._index(self._idx[k]);
            p.push(r);
        });
        if (pi.length == 1) {
            p = p[0];
            if (pi[0] == sort) {
                sort = null;
                if (order == -1)
                    p.reverse();
            }
        } else {
            // TODO: use sort index as intersect base to speedup sorting
            p = tutils.intersectIndexes(p);
        }
        // nowe we have ids, need to convert them to positions
        _(p).forEach(function (_id) {
            range.push(self._store[_id].pos);
        });
    } else {
        if (self._idx[sort]) {
            _.each(self._idx[sort].all(), function (_id) {
                range.push(self._store[_id].pos);
            });
            if (order == -1)
                range.reverse();
            sort = null;
        } else
            range = _.values(self._store).map(function (rec) { return rec.pos; });
    }

    if (self._idx[sort]) {
        var ps = {};
        _(range).each(function (pos) {
            ps[pos] = true;
        });
        range = [];
        _(self._idx[sort].all()).each(function (_id) {
            var pos = self._store[_id].pos;
            if (_(ps).has(pos)) range.push(pos);
        });
        if (order == -1)
            range.reverse();
        sort = null;
    }

    // no sort, no query then return right away
    if (!sort && (_.isEmpty(qt) || _.isEmpty(qt._args))) {
        if (skip !== 0 || limit !== null) {
            var c = Math.min(range.length - skip, limit ? limit : range.length - skip);
            range = range.splice(skip, c);
        }
        return safe.back(cb, null, range);
    }

    // check if we can use simple match or array match function
    var arrayMatch = false;
    if (self._tdb._gopts.searchInArray)
        arrayMatch = true;
    else {
        var fields2 = qt.fields();
        _.each(fields2, function (v, k) {
            if (arFields[k])
                arrayMatch = true;
        });
    }

    var body = arrayMatch ? qt.native3() : qt.native();
    var matcher = Collection._compileMatcher(body);

    // create sort index
    var si = sort ? new tindex(sort, self) : null;
    async.forEachSeries(range, function (pos, cb) {
        if (!sort && limit && res.length >= limit) return cb();

        self._get(pos, safe.sure(cb, function (obj) {
            if (matcher(obj)) {
                if (sort || found >= skip) {
                    if (si) {
                        si.set(obj, pos);
                    } else {
                        res.push(pos);
                    }
                }
                found++;
            }
            cb();
        }));
    }, function (err) {
        if (err) throw err;
        if (sort) {
            res = si.all();
            if (order == -1) {
                res.reverse();
            }
            if (skip !== 0 || limit !== null) {
                var c = Math.min(res.length - skip, limit ? limit : res.length - skip);
                res = res.splice(skip, c);
            }
        }
        cb(null, res);
    });
};


Collection.prototype._find = function (query, fields, skip, limit, sort_, order, hint, arFields, cb) {
    var self = this;
    this._tq.add(function (cb) {
        self.__find(query, fields, skip, limit, sort_, order, hint, arFields, cb);
    }, false, cb);
};


require('./Collection-ext');
require('./Collection-storage');
