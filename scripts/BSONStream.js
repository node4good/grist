'use strict';
var buffalo = require('buffalo');
var util = require('util');
var Transform = require('stream').Transform;
util.inherits(BSONStream, Transform);

function BSONStream(options) {
    if (!(this instanceof BSONStream))
        return new BSONStream(options);

    BSONStream.super_.call(this, options);
    this._buffer = new Buffer(0);
    this._offset = 0;
    this._objs = [];
}

//noinspection JSUnusedGlobalSymbols
BSONStream.prototype._transform = function (chunk, encoding, done) {
    if (!chunk)
        return done();
    this._buffer = Buffer.concat([this._buffer, chunk]);
    var objLength = this._buffer.readInt32LE(this._offset);
    while (this._offset + objLength <= this._buffer.length) {
        var obj = buffalo.parse(this._buffer, this._offset);
        this._offset += obj.__length;
        this._objs.push(obj);
        this.push(obj);
    }
    done();
};

module.exports = BSONStream;
