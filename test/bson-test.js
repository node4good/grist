"use strict";
/*global it,describe */
var fs = require('fs');
var path = require('path');
var assert = require('chai').assert;
var buffalo = require('buffalo');


var tutils = require("./utils");

var gt0sin = 0;
var _dt = null;


describe.only('BSON Parsing', function () {
    this.timeout(10000);
    before(function (done) {
        var test = this;
        tutils.getDb('bson', true, function (err, _db) {
            test._db = _db;
            test.Dbname = "BSON-test";
            test._db.collection(test.Dbname, {}, function (err, _coll) {
                test.coll = _coll;
                test.coll.drop().then(function () {
                    gt0sin = 0;
                    _dt = null;
                    done();
                });
            });
        });
    });

    var filename = path.join(__dirname, '..', 'test-data', 'bson', 'navigations.bson');
    it("Read Stream", function (done) {
        var str = fs.createReadStream(filename, {flags: 'r', autoClose: true});
        var bsonStream = new BSONStream({objectMode: true});
        str.pipe(bsonStream);
        bsonStream.on('data', function (row) {
            console.log(row);
        });
        bsonStream.on('end', function () {
            assert.equal(this._objs.length, 35);
            done();
        });
    });

    it("Read", function (done) {
        var buf = fs.readFileSync(filename);
        buf.__off = 0;
        var objs = [];
        while (buf.__off < buf.length) {
            var obj = buffalo.parse(buf, buf.__off);
            buf.__off += obj.__length;
            objs.push(obj);
        }
        console.log(objs);
        done();
    });


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
});
