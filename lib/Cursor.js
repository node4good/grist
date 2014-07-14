var _ = require('lodash-contrib');
var Promise = require('mpromise');

Cursor.INIT = 0;
Cursor.OPEN = 1;
Cursor.CLOSED = 2;
Cursor.GET_MORE = 3;
function Cursor(tcoll, query, fields, opts) {
    var self = this;
    fields = fields || {};
    opts = opts || {};
    this._query = query || {};
    this._c = tcoll;
    this._i = 0;
    this._skip = 0;
    this._limit = null;
    this._count = null;
    this._items = null;
    this.timeout = 'timeout' in opts ? opts.timeout : true;

    this._fields = {};
    _.each(fields, function (v, k) {
        if (!k && _.isString(v)) {
            k = v;
            v = 1;
        }
        if (v === 0 || v == 1) {
            // _id treated specially
            if (k == "_id" && v === 0) {
                self._fieldsExcludeId = true;
                return;
            }

            if (!self._fieldsType)
                self._fieldsType = v;
            if (self._fieldsType === v) {
                self._fields[k] = v;
            } else if (!self._err)
                self._err = new Error("Mixed set of projection options (0,1) is not valid");
        } else if (!self._err)
            self._err = new Error("Unsupported projection option: " + JSON.stringify(v));
    });

    // _id treated specially
    this._fieldsExcludeId = false;
    if ((self._fieldsType === 0 || self._fieldsType === null) && self._fieldsExcludeId) {
        self._fieldsType = 0;
        self._fields['_id'] = 0;
    }

    this._sort = null;
    if (opts.sort) this.sort(opts.sort);
    this._order = null;
}


Cursor.prototype.isClosed = function () {
    if (!this._items)
        return false;
    return this._i == -1 || this._i >= this._items.length;
};

Cursor.prototype.skip = function (v, cb) {
    var self = this;
    if (!_.isFinite(v)) {
        self._err = new Error("skip requires an integer");
        if (!cb) throw self._err;
    }
    if (self._i) {
        self._err = new Error('Cursor is closed');
        if (!cb) throw self._err;
    }
    if (!self._err)
        this._skip = v;
    if (cb)
        process.nextTick(function () {cb(self._err, self);});
    return this;
};


Cursor.prototype.sort = function (v, cb) {
    if (_.isNumber(cb) || _.isString(cb)) { // handle sort(a,1)
        v = {v: cb};
        cb = null;
    }

    if (this._i) this._err = new Error('Cursor is closed');

    if (this._err) return this;

    if (!_.isObject(v)) {
        if (!_.isString(v)) {
            this._err = new Error("Illegal sort clause, must be of the form [['field1', '(ascending|descending)'], ['field2', '(ascending|descending)']]");
        } else {
            this._sort = v;
            this._order = 1;
        }
    } else {
        if (_.size(v) <= 2) {
            if (_.isArray(v)) {
                if (_.isArray(v[0])) {
                    this._sort = v[0][0];
                    this._order = v[0][1];
                } else {
                    this._sort = v[0];
                    this._order = 1;
                }
            } else {
                this._sort = _.keys(v)[0];
                this._order = v[this._sort];
            }
            if (this._sort) {
                if (this._order == 'asc')
                    this._order = 1;
                if (this._order == 'desc')
                    this._order = -1;
                if (!(this._order == 1 || this._order == -1))
                    this._err = new Error("Illegal sort clause, must be of the form [['field1', '(ascending|descending)'], ['field2', '(ascending|descending)']]");
            }
        } else this._err = new Error("Multi field sort is not supported");
    }

    var self = this;
    if (cb)
        process.nextTick(function () {cb(self._err, self);});

    return this;
};


Cursor.prototype.limit = function (v, cb) {
    var self = this;
    if (!_.isFinite(v)) {
        self._err = new Error("limit requires an integer");
        if (!cb) throw self._err;
    }
    if (self._i) {
        self._err = new Error('Cursor is closed');
        if (!cb) throw self._err;
    }
    if (!self._err) {
        this._limit = v === 0 ? null : Math.abs(v);
    }
    if (cb)
        process.nextTick(function () {cb(self._err, self);});
    return this;
};


Cursor.prototype.nextObject = function (cb) {
    var self = this;
    if (self._err) {
        if (cb) process.nextTick(function () {cb(self._err);});
        return;
    }
    self._ensure(function () {
        if (self._i >= self._items.length)
            return cb(null, null);
        cb(null, self._items[self._i]);
        self._i++;
    });
};


Cursor.prototype.count = function (applySkipLimit, cb) {
    var self = this;
    if (!cb) {
        cb = applySkipLimit;
        applySkipLimit = false;
    }
    if (self._err) {
        if (cb) process.nextTick(function () {cb(self._err);});
        return;
    }
    if ((!self._skip && self._limit === null) || applySkipLimit) {
        self._ensure(function () {
            cb(null, self._items.length);
        });
        return;
    }
    if (self._count !== null) {
        process.nextTick(function () {
            cb(null, self._count);
        });
        return;
    }
    var data = self._c._find(self._query, {}, 0);
    self._count = data.length;
    cb(null, self._count);
};


//noinspection JSUnusedGlobalSymbols
Cursor.prototype.setReadPreference = function (the, cb) {
    var self = this;
    if (self._err) {
        if (cb) process.nextTick(function () {cb(self._err);});
        return;
    }
    return this;
};


Cursor.prototype.batchSize = function (v, cb) {
    var self = this;
    if (!_.isFinite(v)) {
        self._err = new Error("batchSize requires an integer");
        if (!cb) throw self._err;
    }
    if (self._i) {
        self._err = new Error('Cursor is closed');
        if (!cb) throw self._err;
    }
    if (cb) process.nextTick(function () {cb(self._err, self);});
    return this;
};


Cursor.prototype.close = function (cb) {
    var self = this;
    this._items = [];
    this._i = -1;
    this._err = null;
    if (cb)
        process.nextTick(function () {cb(self._err, self);});
    return this;
};


//noinspection JSUnusedGlobalSymbols
Cursor.prototype.rewind = function () {
    this._i = 0;
    return this;
};


Cursor.prototype.toArray = function (cb) {
    if (!_.isFunction(cb))
        throw new Error('Callback is required');

    return this.exec(cb);
};


Cursor.prototype.exec = function (cb) {
    if (this.isClosed())
        this._err = new Error("Cursor is closed");

    if (this._err) return Promise.rejected(this._err);

    return this._ensure().then(function () {
        var iteratorValue = this._i;
        var docs = this._items.slice(iteratorValue);
        return docs;
    }.bind(this)).onResolve(cb).end();
};


Cursor.prototype.each = function (callback) {
    if (!_.isFunction(callback))
        throw new Error('Callback is required');

    var self = this;

    if (self.isClosed())
        self._err = new Error("Cursor is closed");

    if (self._err) {
        if (callback) process.nextTick(function () {
            callback(self._err);
        });
        return;
    }
    return self._ensure().then(function () {
        var slice = self._i ? self._items.slice(self._i, self._items.length) : self._items;
        slice.forEach(function (obj) {
            callback(null, obj);
        });
        self._i = self._items.length;
        callback(null, null);
    }).end();
};


Cursor.prototype._ensure = function (cb) {
    var p = new Promise(cb);
    if (this._items) return p.fulfill();
    var data = this._c._find(this._query, {}, this._skip, this._limit, this._sort, this._order);
    data = data.map(this._projectFields.bind(this));
    this._items = data;
    this._i = 0;
    p.fulfill();
    return p;
};


Cursor.prototype._projectFields = function (obj) {
    if ('_fieldsType' in this) {
        var keys = _(this._fields).keys().map(function (o) { return o.split('.')[0]; }).valueOf();
        if (this._fieldsType === 0)
            obj = _.omit(obj, keys);
        else {
            if (!this._fieldsExcludeId)
                keys.push('_id');
            obj = _.pick(obj, keys);
        }
    }
    return obj;
};


module.exports = Cursor;
