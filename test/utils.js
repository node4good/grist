var main = require('../')({});
var temp = require('temp');
var _ = require('lodash');


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
    var db = new main.Db(paths[tag], {});
    db.open(cb);
};


module.exports.getDbSync = function (tag, db_options, server_options, drop) {
    if (drop)
        delete paths[tag];
    if (!paths[tag]) {
        paths[tag] = temp.mkdirSync(tag);
    }
    return new main.Db(paths[tag], {name: tag});
};


module.exports.openEmpty = function (db, cb) {
    db.open(function (err) {
        if (err) throw err;
        cb();
    });
};


