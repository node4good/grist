'use strict';
var _ = require('lodash-contrib');
var fs = require('fs');
var path = require('path');
var Chanel = require('chanel');
var buffalo = require('buffalo');
var DB = require('..');
var ObjectId = require('../lib/json/ObjectId');

function bsonFileConvert(filename) {
    var buf = fs.readFileSync(filename);
    buf.__off = 0;
    var objs = [];
    while (buf.__off < buf.length) {
        var obj = buffalo.parse(buf, buf.__off);
        buf.__off += obj.__length;
        obj._id = new ObjectId(obj._id);
        objs.push(obj);
    }
    return objs;
}


function bsonDirectoryConvert(direcotry, db, done) {
    if (_.isString(db)) {
        if (!fs.existsSync(db)) fs.mkdirSync(db);
        db = new DB(db, {name: db});
    }
    var files = fs.readdirSync(direcotry).filter(
        function (file) {
            return file && path.extname(file) === '.bson';
        }
    ).map(
        function (file) {
            var colName = path.basename(file, '.bson');
            var filename = path.join(direcotry, file);
            return {col: colName, file: filename};
        }
    );
    var chanel = new Chanel(1);
    files.forEach(function forEachPair(pair) {
        console.log(pair);
        var objs = bsonFileConvert(pair.file);
        var p = db.collection(pair.col).then(function (coll) {
            return coll.drop();
        }).then(function (coll) {
            return coll.insert(objs);
        }).end();
        chanel.push(p);
    });
    chanel(done);
    return db;
}


module.exports.BSONFileConvert = bsonFileConvert;
module.exports.BSONDirectoryConvert = bsonDirectoryConvert;
