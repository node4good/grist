'use strict';

var DB = require('./DB.js');
var ObjectID = require("./ObjectId");
var util = require('util');
var Code = require('./tcode.js').Code;
var Binary = require('./Binary.js').Binary;
var finderFactory = require("./finder");

module.exports = function (optsGlobal) {
  function GDB(path, optsLocal) {
    optsLocal = optsLocal || {};
    optsLocal.name = optsLocal.name || path;
    GDB.superclass.constructor.call(this, path, optsLocal, optsGlobal);
    this.ObjectID = ObjectID;
    this.Code = Code;
    this.Binary = Binary;
    this.Finder = finderFactory(this);
  }

  util.inherits(GDB, DB);
  GDB.superclass = DB.prototype;

  return {
    Db: GDB,
    Collection: require('./Collection-base.js'),
    Code: require('./tcode.js').Code,
    Binary: require('./Binary.js').Binary,
    ObjectID: ObjectID
  };
}
