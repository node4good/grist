'use strict';
var path = require('path');
var _ = require('lodash-contrib');
var Promise = require('mpromise');
var Updater = require('grist-gnash');

var Cursor = require('./Cursor');


function Collection() {
    this._tdb = null;
    this._name = null;
    this._store = {};
    this._id = 1;
    this._tq = null;
    this._idx = {};
    // native mongo db compatibility attrs
    this.collectionName = null;
}
module.exports = Collection;


Collection.simplifyKey = function simplifyKey(key) {
    if (_.isNumber(key)) return key;
    if (_.isString(key)) return key;
    if ('$oid' in key) return key['$oid'];
    if (key.toHexString) return key.toHexString();
    return key.toJSON ? key.toJSON() : String(key);
};


Collection.prototype.init = function (tdb, name, options, callback) {
    var self = this;
    this._tdb = tdb;
    this._name = this.collectionName = name;
    this._filename = path.join(this._tdb._path, this._name + '.json');

    var p = new Promise(callback);
    self._init_store().then(
        function () {
            p.fulfill();
        }
    ).end();
    return p;
};


Collection.prototype.drop = function (cb) {
    return this._remove_batch().onResolve(cb);
};


Collection.prototype.rename = function rename(nname, opts, cb) {
    var self = this;
    var err = self._tdb._nameCheck(nname);
    if (err) throw err;
    this._rename(nname, opts, cb);
};


Collection.prototype.indexes = function (cb) {
    var self = this;
    this._tq.add(function (cb) {
        cb(null, _.values(self._idx));
    }, false, cb);
};


Collection.prototype.insert = function (docs, __, callback) {
    if (_.isFunction(__) && arguments.length == 2) {
        callback = __;
    }
    if (!_.isArray(docs))
        docs = [docs];
    var p = this._put_batch(docs);
    p.onResolve(callback);
    return p.end();
};


Collection.prototype.distinct = function (prop, match, options, callback) {
    if (_.isFunction(options)) {
        callback = options;
    }
    var p = new Promise(callback);
    var docs = this._find(match, prop, options);
    docs = _(docs).pluck(prop).unique().valueOf();
    p.fulfill(docs);
    return p.end();
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
    var docs = _.isEmpty(query) ? this._store : this._find(query, options);
    var p = new Promise(cb);
    p.fulfill(_.size(docs));
    return p.end();

};


var findOpts = ['limit', 'sort', 'fields', 'skip', 'hint', 'timeout', 'batchSize', 'safe', 'w'];

Collection.prototype.findOne = function () {
    var findArgs = _.toArray(arguments);
    var cb = _.isFunction(_.last(findArgs)) ? findArgs.pop() : null;
    var p = new Promise(cb).end();
    var cur = this.find.apply(this, findArgs).limit(1);
    cur.nextObject(p.resolve.bind(p));
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


Collection.prototype.update = function (query, updateCommand, opts, callback) {
    if (!_.isFunction(callback) && _.isFunction(opts)) {
        callback = opts;
        opts = {};
    }
    opts = opts || {};
    if (opts.w > 0 && !_.isFunction(callback)) throw new Error("Callback is required for safe update");

    var self = this;
    var meta = {};
    var p = Promise.fulfilled().then(function () {
        var udocs;
        var multi = opts.multi || false;
        if (!_.isObject(query)) throw new Error("selector must be a valid JavaScript object");
        if (!_.isObject(updateCommand)) throw new Error("document must be a valid JavaScript object");
        _.walk.preorder(updateCommand, function (value, key) {
            if (key === "_id" && multi)
                throw new TypeError("Can't batch update `_id` fields");
        });
        var updater = new Updater(updateCommand, multi);
        var $doc = updater.hasAtomic() ? null : updateCommand;
        var res = self._find(query, null, 0, multi ? null : 1);
        if (_.isEmpty(res)) {
            if (!opts.upsert) return [];
            $doc = $doc || query;
            $doc = self._tdb._cloneDeep($doc);
            updater.update($doc, true);
            if (_.isUndefined($doc._id))
                $doc._id = new self._tdb.ObjectID();

            _.assign(meta, {updatedExisting: false, upserted: $doc._id});
            udocs = [$doc];
        } else {
            udocs = res.map(function (obj) {
                var udoc = $doc;
                if (!$doc) {
                    udoc = obj;
                    updater.update(udoc);
                }
                udoc._id = obj._id;
                return udoc;
            });
            _.assign(meta, {updatedExisting: true});
        }
        return udocs;
    }).then(function (udocs) {
        return self._put_batch(udocs);
    }).then(function (uudocs) {
        meta.n = uudocs.length;
        return new Promise().fulfill(meta.n, meta);
    }).onResolve(callback);
    return p;
};


Collection.prototype.findAndModify = function (query, sort, doc, opts, callback) {
    var self = this;
    if (_.isFunction(opts) && !callback) {
        callback = opts;
        opts = {};
    }
    var p = this.update(query, doc, opts).then(function (n) {
        if (!n) return null;
        var cursor = new Cursor(this, {}, opts.fields || {}, {sort: sort});
        return self._find(query, null, 0, 1, cursor._sort, cursor._order);
    }).onResolve(callback).end();
    return p;
};


Collection.prototype.save = function (doc, __, callback) {
    var args = _.toArray(arguments);
    callback = args.pop();
    callback = _.isFunction(callback) ? callback : null;
    doc = doc || {};
    if (_.isUndefined(doc._id)) {
        doc._id = new this._tdb.ObjectID();
    }
    var p = this._put(doc).onResolve(callback);
    return p.end();
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
    callback = callback || function () {
    };
    var single = opts.single || false;
    var limit = single ? 1 : null;
    var res = self._find(query, null, 0, limit);
    var p = self._remove_batch(res).onResolve(callback);
    return p.end();
};

Collection.prototype.findAndRemove = function (query, sort, opts, cb) {
    if (_.isFunction(sort) && !cb && !opts) {
        cb = sort;
        sort = {};
    } else if (_.isFunction(opts) && !cb) {
        cb = opts;
    }
    var c = new Cursor(this, {}, {}, {sort: sort});
    if (c._err) throw c._err;

    var res = this._find(query, null, 0, 1, c._sort, c._order);
    var ret = this._tdb._cloneDeep(res);
    return this._remove_batch(res).then(function () {
        return ret;
    }).onResolve(cb);
};


Collection.prototype._stop_and_drop = function (cb) {
    this._stop(function () {
        this._drop(cb);
    }.bind(this));
};


//require('./Collection-ext');
require('./Collection-storage');
