var assert = require('assert');
var async = require('async');
var safe = require('safe');
var tutils = require("./utils");


var DELETE_TEST_COL_NAME = 'delete-test';

describe('Delete', function () {
    var db, coll, items, length;

    function checkCount(done) {
        coll.find().count(safe.sure(done, function (count) {
            assert.equal(count, length);
            done();
        }));
    }

    function checkData(done) {
        async.forEach(items, function (item, cb) {
            coll.findOne({ k: item.k }).then(function (doc) {
                if (item.x) {
                    assert.equal(doc, null);
                } else {
                    assert.equal(doc.k, item.k);
                    assert.equal(doc.v, item.v);
                }
                cb();
            }).end();
        }, done);
    }

    before(function (done) {
        tutils.getDb('test', true, safe.sure(done, function (_db) {
            db = _db;
            db.collection(DELETE_TEST_COL_NAME, {}, safe.sure(done, function (_coll) {
                coll = _coll;
                items = [
                    { k: 1, v: 123 },
                    { k: 2, v: 456 },
                    { k: 3, v: 789 },
                    { k: 4, v: 111 }
                ];
                length = items.length;
                coll.insert(items, { w: 1 }, done);
            }));
        }));
    });


    after(function (done) {
        coll.drop(done);
    });


    it('Check count', checkCount);
    it('Check data', checkData);
    it('Delete items', function (done) {
        items[1].x = true;
        items[3].x = true;
        var keys = items.filter(function (x) {
            return x.x;
        }).map(function (x) {
            return x.k;
        });
        length -= keys.length;
        coll.remove({ k: { $in: keys } }, { w: 1 }, done);
    });

    it('Check count after remove', checkCount);
    it('Check data after remove', checkData);
    it('Close database', function (done) {
        db.close(done);
    });


    it('Reopen database', function (done) {
        tutils.getDb('test', false, safe.sure(done, function (_db) {
            db = _db;
            db.collection(DELETE_TEST_COL_NAME, {}, safe.sure(done, function (_coll) {
                coll = _coll;
                done();
            }));
        }));
    });

    it('Check count after reopening db', checkCount);
    it('Check data after reopening db', checkData);

    it('FindAndRemove', function (done) {
        coll.findAndRemove({k: 1}).then(function (objs) {
            assert.equal(objs.length, 1);
            assert.equal(objs[0].k, 1);
            assert.equal(objs[0].v, 123);
            done();
        });
    });
});
