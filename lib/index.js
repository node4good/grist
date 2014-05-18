'use strict';
var fs = require('fs');
var path = require('path');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var async = require('async');
var Collection = require('./Collection-base.js');
var safe = require('safe');
var Promise = require('mpromise');
var ObjectID = require("./ObjectId");
var Binary = require('./Binary.js').Binary;


function TDB(path_, opts) {
    EventEmitter.call(this);
    this._path = path.resolve(path_);
    if (!fs.existsSync(this._path)) fs.mkdirSync(this._path);
    this._cols = {};
    this._name = opts.name || path.basename(path_);
    this.ObjectID = ObjectID;
    this.Binary = Binary;
}
util.inherits(TDB, EventEmitter);


TDB.prototype.open = function (options, cb) {
    // actually do nothing for now, we are inproc
    // so nothing to open/close... collection will keep going on their own
    if (!cb) cb = options;
    cb = cb || function () {
    };
    cb(null, this);
};


TDB.prototype.close = function (forceClose, cb) {
    var self = this;
    if (!cb) cb = forceClose;
    cb = cb || function () {
    };
    async.forEach(_.values(self._cols), function (c, cb) {
        c._stop(cb);
    }, function (err) {
        if (err) throw err;
        self._cols = {};
        cb(null, this);
    });
};


TDB.prototype.collection = function (cname, opts, cb) {
    var db = this;
    opts = opts || {};
    var err = this._nameCheck(cname);
    if (err) throw err;
    var c = db._cols[cname];
    if (!c && opts.strict) throw new Error("Collection does-not-exist does not exist. Currently in safe mode.");
    if (c) {
        return Promise.fulfilled(c).onResolve(cb);
    }

    var newc = new Collection();
    var pret = newc.init(db, cname, opts).then(
        function () {
            db._cols[cname] = newc;
            return newc;
        }
    );
    return pret.onResolve(cb);
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


TDB.prototype.collectionNames = function (opts, cb) {
    var self = this;
    if (!cb) {
        cb = opts;
        opts = {};
    }
    fs.readdir(self._path, safe.sure(cb, function (files) {
        // some collections ca be on disk and some only in memory, we need both
        files = _.union(files, _.keys(self._cols)).filter(function (fn) {
            return fn[0] !== '.';
        });
        cb(null, _(files).map(function (e) {
            return opts.namesOnly ? e : {name: self._name + "." + e};
        }).value());
    }));
};


TDB.prototype.collections = function (callback) {
    var self = this;
    self.collectionNames({namesOnly: 1}, function (err, names) {
        if (err) throw err;
        async.forEach(names, function (cname, cb) {
            self.collection(cname, {}, cb);
        }, function (err) {
            callback(err, _.values(self._cols));
        });
    });
};


TDB.prototype.dropCollection = function (cname, cb) {
    var self = this;
    var c = this._cols[cname];
    delete self._cols[cname];
    if (!c) {
        throw new Error("Collection not found: " + cname);
    }
    c._stop_and_drop(cb);
};


//noinspection JSUnusedGlobalSymbols
TDB.prototype.dropDatabase = function (callback) {
    var self = this;
    self.collections(function (err, collections) {
        if (err) throw err;
        async.forEach(collections, function (c, cb) {
            self.dropCollection(c.collectionName, cb);
        }, callback);
    });
};


//noinspection JSUnusedGlobalSymbols
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

    });
};


module.exports = TDB;
