var _ = require('lodash');
var assert = require('assert');
var async = require('async');
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
        var iterator = coll.insert.bind(coll);
        var schema;
        var queue = async.queue(function (value, cb) {
            if (_.isEmpty(value)) return done();
            iterator(value, cb);
        }, 1);
        var gunzip = zlib.createGunzip();
        fs.createReadStream(sample).pipe(gunzip);
        csv().from.stream(gunzip).on('record', function (row, index) {
            if (index === 0) schema = row;
            else {
                var value = { id: index };
                row.forEach(function (item, i) {
                    value[schema[i]] = item;
                });
                queue.push(value);
            }
        }).on('end', function () {
            queue.push({});
        });
    });


    it("Has right size", function (done) {
        coll.count().then(function (count) {
            assert.equal(count, rowcount);
            done();
        });
    });
});
