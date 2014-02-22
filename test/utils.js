//require('trace');

global._ = require('lodash-contrib');
global.expect = require('chai').expect;

var fs = require('fs');
var path = require('path');
var DB = require('../');


var paths = {};


module.exports.getDbSync = function (tag, db_options, server_options, drop) {
    if (drop)
        delete paths[tag];
    if (!paths[tag]) {
        var tempName = path.join('test-data', tag);
        if (!fs.existsSync('test-data')) fs.mkdirSync('test-data');
        if (!fs.existsSync(tempName)) fs.mkdirSync(tempName);
        paths[tag] = tempName;
    }
    return new DB(paths[tag], {name: tag});
};


module.exports.getDb = function (tag, drop, cb) {
    var db = module.exports.getDbSync(tag, null, null, drop);
    module.exports.openEmpty(db, cb);
};


module.exports.openEmpty = function (db, cb) {
    db.open(function (err) {
        if (err) throw err;
        cb(null, db);
    });
};


module.exports.startDb = module.exports.stopDb = function (cb) { cb(); };
