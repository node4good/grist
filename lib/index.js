'use strict';

var DB = require('./tdb.js');
var ObjectID = require("./ObjectId");
var util = require('util');
var Code = require('./tcode.js').Code;
var Binary = require('./tbinary.js').Binary;
var FinderFactory = require("./finder");

function GDB(path, optsLocal) {
    GDB.superclass.constructor.call(this, path, optsLocal);
    this.ObjectID = ObjectID;
    this.Code = Code;
    this.Binary = Binary;
    this.Finder = FinderFactory(this);
}
util.inherits(GDB, DB);
GDB.superclass = DB.prototype;

module.exports = {
    Db: GDB,
    Collection: require('./tcoll.js'),
    Code: require('./tcode.js').Code,
    Binary: require('./tbinary.js').Binary,
    ObjectID: ObjectID
};
