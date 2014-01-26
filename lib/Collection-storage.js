"use strict";
var fs = require('fs');
var path = require('path');
var async = require('async');
var Promise = require('mpromise');
var Collection = module.parent.exports;


Collection.prototype._init_store = function _init_store(pos, deleted, callback) {
    var self = this;
    var p = new Promise(callback);
    fs.open(self._filename, "a+", function (err, fd) {
        if (err) throw err;
        self._fd = fd;
        var b1 = new Buffer(45);
        async.whilst(
            function () { return self._fsize === null; },
            function (cb) {
                fs.read(fd, b1, 0, 45, pos, function (err, bytes, data) {
                    if (err) throw err;
                    if (bytes === 0) {
                        self._fsize = pos;
                        return cb();
                    }
                    var h1 = JSON.parse(data.toString());
                    h1.o = parseInt(h1.o, 10);
                    h1.k = parseInt(h1.k, 10);
                    var b2 = new Buffer(h1.k);
                    fs.read(fd, b2, 0, h1.k, pos + 45 + 1, function (err, bytes, data) {
                        if (err) throw err;
                        var k = JSON.parse(data.toString());
                        self._id = k._uid;
                        if (k._a == 'del') {
                            delete self._store[k._id];
                            deleted++;
                        } else {
                            if (self._store[k._id]) deleted++;
                            self._store[k._id] = { pos: pos, sum: k._s };
                        }
                        pos += 45 + 3 + h1.o + h1.k;
                        if (err) throw new Error(self._name + ": Error during load - " + err.toString());
                        cb();
                    });
                });
            },
            p.resolve.bind(p)
        );
    });
    return p;
};


Collection.prototype._put_write = function _put_write(sobj, key, item, buf, remove, cb) {
    var rec = this._store[key._id];
    if (rec && rec.sum == key._s) return cb();
    fs.write(this._fd, buf, 0, buf.length, this._fsize, function (err, written) {
        if (err) throw err;
        if (remove)
            delete this._store[key._id];
        else
            this._store[key._id] = { pos: this._fsize, sum: key._s };

        if (remove || sobj.length > this._cmaxobj)
            this._cache.unset(this._fsize);
        else
            this._cache.set(this._fsize, item);
        this._fsize += written;
        cb();
    }.bind(this));
};


Collection.prototype._get = function _get_read(pos, cb) {
    var p = new Promise(cb);

    var cache = this._cache;
    var cached = cache.get(pos);
    if (cached) {
        p.fulfill(cached);
        return p;
    }

    var b1 = new Buffer(45);
    fs.read(this._fd, b1, 0, 45, pos, function (err, bytes, data) {
        if (err) throw err;
        var h1 = JSON.parse(data.toString());
        h1.o = parseInt(h1.o, 10);
        h1.k = parseInt(h1.k, 10);
        var b2 = new Buffer(h1.o);
        var next_position = pos + 45 + 2 + h1.k;
        fs.read(this._fd, b2, 0, h1.o, next_position, function (err, bytes, data) {
            if (err) throw err;
            var obj = this._unwrapTypes(JSON.parse(data.toString()));
            if (bytes <= this._cmaxobj) cache.set(pos, obj);
            p.fulfill(obj);
        }.bind(this));
    }.bind(this));
    return p;
};


Collection.prototype._stop = function (cb) {
    // this will prevent any tasks processed on this instance
    this._tq._stoped = true;
    if (!this._fd) {
        cb(null, false);
        return;
    }
    fs.close(this._fd, function (err) {
        if (err) throw err;
        cb(null, true);
    });
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
