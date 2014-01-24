var fs = require('fs');
var path = require('path');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var async = require('async');
var tcoll = require('./tcoll.js');

var safe = {sure: function (cb, w) {return function (err) { if (err) throw err; var args = [].slice.call(arguments, 1); w.apply(null, args); }; }};

function TDB(path_, opts, gopts) {
    this._gopts = gopts;
    this._path = path.resolve(path_);
    this._cols = {};
    this._name = opts.name || path.basename(path_);
}
util.inherits(TDB, EventEmitter);


TDB.prototype.open = function (options, cb) {
    // actually do nothing for now, we are inproc
    // so nothing to open/close... collection will keep going on their own
    if (!cb) cb = options;
    cb = cb || function () {};
    cb(null, this);
};


TDB.prototype.close = function (forceClose, cb) {
    var self = this;
    if (!cb) cb = forceClose;
    cb = cb || function () {};
    // stop any further operations on current collections
    async.forEach(_.values(self._cols), function (c, cb) {
        c._stop(cb);
    }, safe.sure(cb, function () {
        // and clean list
        self._cols = {};
        cb(null, this);
    }));
};


TDB.prototype.createIndex = function (colName) {
    var c = this._cols[colName];
    if (!c) throw new Error("Collection doesn't exists");
    var nargs = Array.prototype.slice.call(arguments, 1, arguments.length - 1);
    c.createIndex.apply(c, nargs);
};


TDB.prototype.collection = function (cname, opts, cb) {
    return this._collection(cname, opts, false, cb);
};


TDB.prototype.createCollection = function (cname, opts, cb) {
    return this._collection(cname, opts, true, cb);
};

TDB.prototype._nameCheck = function (cname) {
    var err = null;
    if (!_.isString(cname))
        err = new Error("collection name must be a String");
    if (!err && cname.length === 0)
        err = new Error("collection names cannot be empty");
    if (!err && cname.indexOf("$") != -1)
        err = new Error("collection names must not contain '$'");
    if (!err) {
        var di = cname.indexOf(".");
        if (di === 0 || di == cname.length - 1)
            err = new Error("collection names must not start or end with '.'");
    }
    if (!err && cname.indexOf("..") != -1)
        err = new Error("collection names cannot be empty");
    return err;
};


TDB.prototype._collection = function (cname, opts, create, cb) {
    var err = this._nameCheck(cname);

    if (!cb) {
        cb = opts;
        opts = {};
    }
    cb = cb || function () {};
    if (err)
        return cb(err);
    var self = this;
    var c = self._cols[cname];
    if (c) {
        cb((opts.strict && create) ? new Error("Collection test_strict_create_collection already exists. Currently in safe mode.") : null, c);
        return c;
    } else if (!create && opts.strict) {
        return cb(new Error("Collection does-not-exist does not exist. Currently in safe mode."));
    }
    c = new tcoll();
    c.init(this, cname, opts, safe.sure(cb, function () {
        self._cols[cname] = c;
        cb(null, c);
    }));
    return c;
};


TDB.prototype.collectionNames = function (opts, cb) {
    var self = this;
    if (!cb) {
        cb = opts;
        opts = {};
    }
    fs.readdir(self._path, safe.sure(cb, function (files) {
        // some collections ca be on disk and some only in memory, we need both
        files = _.union(files, _.keys(self._cols));
        cb(null, _(files).map(function (e) { return opts.namesOnly ? e : {name: self._name + "." + e};}).value());
    }));
};


TDB.prototype.collections = function (cb) {
    var self = this;
    self.collectionNames({namesOnly: 1}, safe.sure(cb, function (names) {
        async.forEach(names, function (cname, cb) {
            self.collection(cname, cb);
        }, safe.sure(cb, function () {
            cb(null, _.values(self._cols));
        }));
    }));
};


TDB.prototype.dropCollection = function (cname, cb) {
    var self = this;
    var c = this._cols[cname];
    if (!c) {
        throw new Error("ns not found");
    }
    c._stop(safe.sure(cb, function (ondisk) {
        delete self._cols[cname];
        if (ondisk)
            fs.unlink(path.join(self._path, cname), safe.sure(cb, function () {
                cb(null, true);
            }));
        else
            cb(null, true);
    }));
};


TDB.prototype.dropDatabase = function (cb) {
    var self = this;
    self.collections(safe.sure(cb, function (collections) {
        async.forEach(collections, function (c, cb) {
            self.dropCollection(c.collectionName, cb);
        }, cb);
    }));
};


TDB.prototype.renameCollection = function (on, nn, opts, cb) {
    if (!cb) {
        cb = opts;
    }
    cb = cb || _.identity;
    var old = this._cols[on];
    if (old)
        old.rename(nn, {}, cb);
    else
        cb();
};


TDB.prototype._cloneDeep = function (obj) {
    var self = this;
    return _.cloneDeep(obj, function (c) {
        if (c instanceof self.ObjectID)
            return new c.constructor(c.toString());

        if (c instanceof self.Binary)
            return new c.constructor(new Buffer(c.value(true)));

        return {};
    });
};


module.exports = TDB;
