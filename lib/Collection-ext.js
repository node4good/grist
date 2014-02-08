/* jshint ignore:start */
var safe = require('safe');
var async = require('async');
Collection = module.parent.exports;


Collection.prototype.mapReduce = function (map, reduce, opts, cb) {
    var self = this;
    if (_.isFunction(opts)) {
        cb = opts;
        opts = {};
    }

    if (!opts.out) throw new Error('the out option parameter must be defined');
    if (!opts.out.inline && !opts.out.replace) throw new Error('the only supported out options are inline and replace');

    code2fn(opts.scope);

    var m = {};
    var finalize = null;
    with (opts.scope || {}) {
        try {
            if (map instanceof Code) {
                with (map.scope) {
                    map = eval('(' + map.code + ')');
                }
            } else {
                map = eval('(' + map + ')');
            }
            if (reduce instanceof Code) {
                with (reduce.scope) {
                    reduce = eval('(' + reduce.code + ')');
                }
            } else {
                reduce = eval('(' + reduce + ')');
            }
            if (finalize instanceof Code) {
                with (finalize.scope) {
                    finalize = eval('(' + finalize.code + ')');
                }
            } else {
                finalize = eval('(' + opts.finalize + ')');
            }
        } catch (e) {
            throw e;
        }
    }

    self.find(opts.query, null, { limit: opts.limit, sort: opts.sort }, safe.sure(cb, function (c) {
        var doc;
        async.doUntil(
            function (cb) {
                c.nextObject(safe.trap_sure(cb, function (_doc) {
                    doc = _doc;
                    if (doc) map.call(doc);
                    return cb();
                }));
            },
            function () {
                return doc === null;
            },
            safe.trap_sure(cb, function () {
                    _(m).each(function (v, k) {
                        v = v.length > 1 ? reduce(k, v) : v[0];
                        if (finalize) v = finalize(k, v);
                        m[k] = v;
                    });

                    var stats = {};
                    if (opts.out.inline) return process.nextTick(function () {
                        cb(null, _.values(m), stats); // execute outside of trap
                    });

                    // write results to collection
                    async.waterfall([
                        function (cb) {
                            self._tdb.collection(opts.out.replace, { strict: 1 }, function (err, col) {
                                if (err) return cb(null, null);
                                col.drop(cb);
                            });
                        },
                        function (arg, cb) {
                            self._tdb.collection(opts.out.replace, {}, cb);
                        },
                        function (col, cb) {
                            var docs = [];
                            _(m).each(function (value, key) {
                                var doc = {
                                    _id: key,
                                    value: value
                                };
                                docs.push(doc);
                            });
                            col.insert(docs, safe.sure(cb, function () {
                                if (opts.verbose) cb(null, col, stats);
                                else cb(null, col);
                            }));
                        }
                    ], cb);
                }
            )); // doUntil
    }));
};


Collection.prototype.group = function (keys, condition, initial, reduce, finalize, command, options, callback) {
    var self = this;

    var args = Array.prototype.slice.call(arguments, 3);
    callback = args.pop();
    reduce = args.length ? args.shift() : null;
    finalize = args.length ? args.shift() : null;
    options = args.length ? args.shift() : {};

    if (!_.isFunction(finalize)) {
        finalize = null;
    }

    code2fn(options.scope);

    with (options.scope || {}) {
        try {
            if (_.isFunction(keys)) keys = eval('(' + keys + ')');
            else if (keys instanceof Code) {
                with (keys.scope) {
                    keys = eval('(' + keys.code + ')');
                }
            }
            if (reduce instanceof Code) {
                with (reduce.scope) {
                    reduce = eval('(' + reduce.code + ')');
                }
            } else reduce = eval('(' + reduce + ')');
            if (finalize instanceof Code) {
                with (finalize.scope) {
                    finalize = eval('(' + finalize.code + ')');
                }
            } else finalize = eval('(' + finalize + ')');
        } catch (e) {
            return callback(e);
        }
    }

    var m = {};
    self.find(condition, safe.sure(callback, function (c) {
        var doc;
        async.doUntil(
            function (cb) {
                c.nextObject(safe.sure(cb, function (_doc) {
                    doc = _doc;
                    if (!doc) return cb();
                    var keys2 = keys;
                    if (_.isFunction(keys)) keys2 = keys(doc);
                    if (!_.isArray(keys2)) {
                        var keys3 = [];
                        _(keys2).each(function (v, k) {
                            if (v) keys3.push(k);
                        });
                        keys2 = keys3;
                    }
                    var key = {};
                    _(keys2).each(function (k) {
                        key[k] = doc[k];
                    });
                    var skey = JSON.stringify(key);
                    var obj = m[skey];
                    if (!obj) obj = m[skey] = _.extend({}, key, initial);
                    try {
                        reduce(doc, obj);
                    } catch (e) {
                        return cb(e);
                    }
                    cb();
                }));
            },
            function () {
                return doc === null;
            },
            safe.sure(callback, function () {
                var result = _.values(m);
                if (finalize) {
                    _(result).each(function (value) {
                        finalize(value);
                    });
                }
                callback(null, result);
            })
        );
    }));
};
/* jshint ignore:end */
