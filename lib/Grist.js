'use strict';
var _ = require('lodash-contrib');
var fs = require('fs');
var Path = require('path');
var MPromise = require('mpromise');
var rimraf = require('rimraf');
var Collection = require('./Collection.js');
var ObjectID = require("./json/ObjectId");


function TDB(path_, opts) {
    this._path = Path.resolve(path_);
    if (!fs.existsSync(this._path)) fs.mkdirSync(this._path);
    this._cols = {};
    this._name = opts.name || Path.basename(path_);
    this.ObjectID = ObjectID;
}


TDB.prototype.open = function (options, cb) {
    // actually do nothing for now, we are inproc
    // so nothing to open/close... collection will keep going on their own
    if (!cb) cb = options;
    cb = cb || _.noop;
    cb(null, this);
};


TDB.prototype.close = function (forceClose, cb) {
    if (!cb) cb = forceClose;
    cb = cb || _.noop;
    cb();
};


TDB.prototype.collection = function (cname, opts, cb) {
    var db = this;
    opts = opts || {};
    var c = db._cols[cname];
    if (!c && opts.strict) throw new Error("Collection does-not-exist does not exist. Currently in safe mode.");
    if (c) {
        return MPromise.fulfilled(c).onResolve(cb);
    }

    var newC = new Collection(db, cname);
    if (opts.clean)
        newC.drop();
    else
        newC.init();
    db._cols[cname] = newC;
    return MPromise.fulfilled(newC).onResolve(cb);
};


TDB.prototype.collectionNames = function () {
    var files = fs.readdirSync(this._path);
    // some collections ca be on disk and some only in memory, we need both
    var namesInMemory = _.keys(this._cols);
    var names = _(files)
        .union(namesInMemory)
        .filter(function (fn) {
            return fn[0] !== '.';
        })
        .map(Path.basename)
        .valueOf();
    return names;
};


TDB.prototype.collections = function (callback) {
    var names = this.collectionNames();
    var p = MPromise.fulfilled();
    while (names.length) {
        var name = names.pop();
        p = p.then(this.collection(name));
    }
    p.onResolve(callback);
    return p;
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


TDB.prototype.dropDatabase = function (callback) {
    var p = new MPromise;
    rimraf(this._path, function (err) {
        p.resolve(err);
    });
    return p.onResolve(callback);
};


TDB.prototype._cloneDeep = function (obj) {
    var self = this;
    return _.cloneDeep(obj, function (c) {
        if (c instanceof self.ObjectID)
            return new c.constructor(c.toString());
    });
};


module.exports = TDB;
