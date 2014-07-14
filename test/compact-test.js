var assert = require('assert');
var async = require('async');
var fs = require('fs');
var safe = require('safe');
var tutils = require('./utils');
var _ = require('lodash-contrib');
var tingodb = require('..');


var COMPACT_TEST_COL_NAME = 'Compact';

describe('Compact', function () {
    var db, coll, items, length, fsize;

    before(function (done) {
        tutils.getDb('test', true, function (err, _db) {
            expect(err).to.be.null;
            db = _db;
            db.collection(COMPACT_TEST_COL_NAME, {}).then(
                function (_coll) {
                    coll = _coll;
                    return coll.drop();
                }
            ).then(
                function () {
                    items = _.times(100, function (n) {
                        return { k: n, v: _.random(100) };
                    });
                    return coll.insert(items, { w: 1 }, done);
                }
            ).onResolve(done);
        });
    });


    it('Count all docs', function (done) {
        coll.find().toArray(function (err, items) {
            length = items.length;
            done();
        });
    });

    it('Update some items', function (done) {
        var docs = _.times(30, function () {
            var idx = _.random(items.length - 1);
            var doc = items[idx];
            doc.v = _.random(101, 200);
            return doc;
        });
        async.forEachSeries(docs, function (doc, cb) {
            coll.update({ k: doc.k }, doc, { w: 1 }, cb);
        }, done);
    });

    it('Delete some items', function (done) {
        var count = 50;
        var keys = _.times(count, function () {
            for (; ;) {
                var idx = _.random(items.length - 1);
                var doc = items[idx];
                if (!doc.x) {
                    doc.x = true;
                    return doc.k;
                }
            }
        });
        length -= count;
        coll.remove({ k: { $in: keys } }, { w: 1 }, done);
    });

    it('Update some items again', function (done) {
        var docs = _.times(30, function () {
            var idx = _.random(items.length - 1);
            var doc = items[idx];
            if (doc.x) {
                delete doc.x;
                length++;
            }
            doc.v = _.random(201, 300);
            return doc;
        });
        async.forEachSeries(docs, function (doc, cb) {
            coll.update({ k: doc.k }, doc, { upsert: true, w: 1 }, cb);
        }, function (err) {
            if (err) throw err;
            done();
        });
    });
    it('Check count', function checkCount(done) {
        coll.find().count(function (err, count) {
            if (err) throw err;
            expect(count).to.equal(length);
            done();
        });
    });
    it('Check data', function checkData(done) {
        async.forEachSeries(items, function (item, cb) {
            coll.findOne({ k: item.k }, safe.sure(cb, function (doc) {
                if (item.x) {
                    assert.equal(doc, null);
                } else {
                    assert.equal(doc.k, item.k);
                    assert.equal(doc.v, item.v);
                }
                cb();
            }));
        }, done);
    });
    it('Close database', function (done) {
        db.close(done);
    });

    it('Remember collection size', function (done) {
        fs.stat(coll._filename, safe.sure(done, function (stats) {
            fsize = stats.size;
            done();
        }));
    });
    it('Reopen database', function (done) {
        tutils.getDb('test', false, safe.sure(done, function (_db) {
            db = _db;
            done();
        }));
    });
    it('Get test collection', function (done) {
        db.collection(COMPACT_TEST_COL_NAME, {}, safe.sure(done, function (_coll) {
            coll = _coll;
            done();
        }));
    });
    it('Check count after reopening db', function checkCount(done) {
        coll.find().count(function (err, count) {
            if (err) throw err;
            expect(count).to.equal(length);
            done();
        });
    });
    it('Check data after reopening db', function checkData(done) {
        async.forEachSeries(items, function (item, cb) {
            coll.findOne({ k: item.k }, safe.sure(cb, function (doc) {
                if (item.x) {
                    assert.equal(doc, null);
                } else {
                    assert.equal(doc.k, item.k);
                    assert.equal(doc.v, item.v);
                }
                cb();
            }));
        }, done);
    });
    it('Check collection size', function (done) {
        fs.stat(coll._filename, safe.sure(done, function (stats) {
            assert(stats.size <= fsize, stats.size + " should be less than " + fsize);
            done();
        }));
    });


    describe('Update+Hash', function () {
        var db, coll, fsize;
        before(function (done) {
            tutils.getDb('test', true, function (err, _db) {
                expect(err).to.be.null;
                db = _db;
                db.collection('Update+Hash', {}).then(
                    function (_coll) {
                        coll = _coll;
                        return coll.drop();
                    }
                ).then(
                    function () {
                        return coll.insert({ k: 1, v: 123 }, { w: 1 });
                    }
                ).onResolve(done);
            });
        });

        it('Remember collection size', function (done) {
            fs.stat(coll._filename, safe.sure(done, function (stats) {
                fsize = stats.size;
                done();
            }));
        });

        it('Collection should grow', function (done) {
            fs.stat(coll._filename, safe.sure(done, function (stats) {
                assert(stats.size >= fsize);
                fsize = stats.size;
                done();
            }));
        });

        it('Update with the same value', function (done) {
            coll.update({ k: 1 }, { k: 1, v: 456 }, { w: 1 }, done);
        });

        it('Update data again', function (done) {
            coll.update({ k: 1 }, { k: 1, v: 789 }, { upsert: true, w: 1 }, done);
        });

        it('Ensure data is correct', function (done) {
            coll.find({ k: 1 }).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 1);
                assert.equal(docs[0].v, 789);
                done();
            }));
        });
    });


    describe('Store', function () {
        it('Operations must fail if db is linked to not existent path', function (done) {
            var Db = tingodb;
            var db;
            try {
                db = new Db('/tmp/some_unexistant_path_667676qwe', {});
            } catch (e) {
                assert(e);
                done();
                return;
            }
            var c = db.collection('test');
            c.remove({}, function (err) {
                assert(err);
                c.insert({  name: 'Chiara', surname: 'Mobily', age: 22 }, function (err) {
                    assert(err);
                    done();
                });
            });
        });
    });

});
