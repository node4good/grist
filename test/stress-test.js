"use strict";
var assert = require('assert');
var csv = require('csv');
var fs = require('fs');
var zlib = require('zlib');
var tutils = require("./utils");

var ROW_COUNT = 500;
var idx = 0;


describe.skip('Stress', function () {
    this.timeout(30 * 1000);
    var coll;
    var collName = 'Import-' + ROW_COUNT;
    var sample = __dirname + '/sample-data/' + ROW_COUNT + '.csv.gz';

    before(function (done) {
        tutils.getDb('test', true, function (err, db) {
            if (err) throw err;
            db.collection(collName, {}, function (err, _coll) {
                if (err) throw err;
                coll = _coll;
                coll.drop(done);
            });
        });
    });


    it("Load", function (done) {
        fs.createReadStream(sample)
            .pipe(zlib.createGunzip())
            .pipe(csv.parse({columns: true}))
            .pipe(csv.transform(function (row, callback) {
                row._id = idx++;
                coll.insert(row).onResolve(callback);
            }), {parallel: 1})
            .on('end', function () {
                assert.equal(idx, ROW_COUNT);
                done();
            })
            .resume();
    });


    it("Has right size", function (done) {
        coll.count().then(function (count) {
            assert.equal(count, ROW_COUNT);
            done();
        }).catch();
    });


    it("Check", function (done) {
        idx = 0;
        fs.createReadStream(sample)
            .pipe(zlib.createGunzip())
            .pipe(csv.parse({columns: true}))
            .pipe(csv.transform(function (row, callback) {
                row._id = idx++;
                coll.findOne({_id: row._id}).onResolve(function (err, datum) {
                    if (err) throw err;
                    assert.deepEqual(datum, row);
                    callback();
                });
            }))
            .on('end', function () {
                assert.equal(idx, ROW_COUNT);
                done();
            })
            .resume();
    });
});
