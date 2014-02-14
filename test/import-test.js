var assert = require('assert');
var csv = require('csv');
var fs = require('fs');
var zlib = require('zlib');
var tutils = require("./utils");

var rowcount = 500;


describe('Stress', function () {
    var coll;
    var collName = 'Import-' + rowcount;
    var sample = __dirname + '/sample-data/' + rowcount + '.csv.gz';

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
        var fileStream = fs.createReadStream(sample).pipe(zlib.createGunzip());
        csv().from
            .stream(fileStream, {columns: true})
            .transform(function (row, idx, callback) {
                row._id = idx;
                coll.insert(row, callback);
            })
            .on('end', function (cnt) {
                assert.equal(cnt, rowcount);
                done();
            });
    });


    it("Has right size", function (done) {
        coll.count().then(function (count) {
            assert.equal(count, rowcount);
            done();
        });
    });


    it("Check", function (done) {
        var fileStream = fs.createReadStream(sample).pipe(zlib.createGunzip());
        csv().from
            .stream(fileStream, {columns: true})
            .transform(function (row, idx, callback) {
                row._id = idx;
                coll.findOne({_id: idx}, function (err, datum) {
                    if (err) throw err;
                    assert.deepEqual(datum, row);
                    callback();
                });
            })
            .on('end', function (cnt) {
                assert.equal(cnt, rowcount);
                done();
            });
    });
});
