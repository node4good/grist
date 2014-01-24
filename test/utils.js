var main = require('../')({});
var main_native = require('../')({ nativeObjectID: true });
var main_array = require('../')({ searchInArray: true });
var temp = require('temp');
var _ = require('lodash');
var safe = require('safe');


var cfg = {};
module.exports.setConfig = function (cfg_) {
    _.defaults(cfg_, cfg);
    cfg = cfg_;
};

module.exports.startDb = module.exports.stopDb = function (cb) { cb(); };

var paths = {};

module.exports.getDb = function (tag, drop, cb) {
    if (drop)
        delete paths[tag];
    if (!paths[tag]) {
        paths[tag] = temp.mkdirSync(tag);
    }
    var tingodb = cfg.nativeObjectID ? main_native : (cfg.searchInArray ? main_array : main);
    var db = new tingodb.Db(paths[tag], {});
    db.open(cb);
};

module.exports.getDbSync = function (tag, db_options, server_options, drop) {
    if (drop)
        delete paths[tag];
    if (!paths[tag]) {
        paths[tag] = temp.mkdirSync(tag);
    }
    var tingodb = cfg.nativeObjectID ? main_native : (cfg.searchInArray ? main_array : main);
    return new tingodb.Db(paths[tag], {name: tag});
};

module.exports.openEmpty = function (db, cb) {
    db.open(safe.sure(cb, function () {
        if (cfg.mongo) {
            db.dropDatabase(cb);
        } else {
            // nothing to do: for tingodb we can request
            // empty database with getDbSync
            cb();
        }
    }));
};

module.exports.getDbPackage = function () {
    var tingodb = cfg.nativeObjectID ? main_native : (cfg.searchInArray ? main_array : main);
    return cfg.mongo ? require('mongodb') : tingodb;
};
