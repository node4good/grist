"use strict";
var _ = require('lodash');
var fs = require('fs');
var path = require('path');
var Promise = require('mpromise');
var ObjectId = require('./ObjectId');
var obj_sort = require('./json/sort-object');
var JSON5 = require('./json/json-serialize');
var Collection = module.parent.exports;


Collection.prototype._init_store = function _init_store(__, ___, callback) {
    var p = new Promise(callback);
    this._store = {};
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
    return p;
};


Collection.prototype._put = function (item, __, callback) {
    var p = new Promise(callback);
    item.pos = Collection.simplifyKey(item._id);
    this._store[item.pos] = obj_sort(item);
    var data = JSON.stringify(this._store, null, '\t');
    fs.writeFile(this._filename, data, function (err, written) {
        if (err) throw err;
        this._fsize = written;
        p.fulfill(item);
    }.bind(this));
    return p;
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
