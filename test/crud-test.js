"use strict";
/*global it,describe */
var assert = require('assert');
var _ = require('lodash-contrib');
var safe = require('safe');
var tutils = require("./utils");


describe('C.R.U.D.', function () {
    var db, coll;

    before(function (done) {
        tutils.getDb('test', true, function (err, _db) {
            db = _db;
            db.collection("test", {}, function (err, _coll) {
                coll = _coll;
                done();
            });
        });
    });

    describe('Should save', function () {
        var obj;
        it('create new', function (done) {
            obj = {i: 1, j: 1};
            coll.save(obj, done);
        });
        it('id is assigned', function (done) {
            assert(obj._id);
            done();
        });
        it('modify it', function (done) {
            obj.i++;
            coll.save(obj).then(
                function () {
                    return coll.findOne({_id: obj._id});
                }
            ).then(
                function (obj1) {
                    assert.deepEqual(obj1, obj);
                }
            ).onResolve(done);
        });
        it('delete it', function (done) {
            coll.remove({_id: obj._id}, safe.sure(done, function () {
                coll.findOne({_id: obj._id}, {sort: {i: 1}}, safe.sure(done, function (obj1) {
                    assert(!obj1);
                    done();
                }));
            }));
        });
    });

    describe('Should update', function () {
        it('create with upsert and $set apply $set to query', function (done) {
            var obj = this.obj = {j: 3, c: "multi", a: [1, 2, 3, 4, 5]};
            var q = {i: 2};
            coll.remove(q).then(
                function () {
                    return coll.update(q, {$set: obj}, {upsert: true});
                }
            ).then(
                function (n, r) {
                    assert.equal(n, 1);
                    assert.equal(r.updatedExisting, false);
                    assert(r.upserted);
                    return coll.findOne(q);
                }
            ).then(
                function (obj1) {
                    assert.equal(obj1.i, q.i);
                    done();
                }
            ).end();
        });
        it('update array field is possible', function (done) {
            coll.update({i: 2}, {$set: {a: [1, 2]}}).then(
                function (n, r) {
                    assert.equal(n, 1);
                    assert.equal(r.updatedExisting, true);
                    return coll.findOne({i: 2});
                }
            ).then(
                function (obj1) {
                    assert.deepEqual([1, 2], obj1.a);
                    done();
                }
            ).end();
        });
        it('upsert one more did not touch initial object', function (done) {
            var obj = this.obj = {j: 4, i: 3, c: "multi", a: [1, 2, 3, 4, 5]};
            var clone = _.cloneDeep(this.obj);
            var q = {i: 3};
            coll.remove(q).then(
                function () {
                    return coll.update(q, {$set: obj}, {upsert: true});
                }
            ).then(
                function (n, r) {
                    assert.equal(n, 1);
                    assert.equal(r.updatedExisting, false);
                    assert(r.upserted);
                    return coll.findOne(q);
                }
            ).then(
                function (obj1) {
                    assert.deepEqual(obj, clone);
                    clone._id = obj1._id;
                    assert.deepEqual(obj1, clone);
                    done();
                }
            ).end();
        });
        it('modify multi changes only specific field for many documents', function (done) {
            coll.update({c: "multi"}, {$set: {a: []}}, {multi: true}).then(
                function (n, r) {
                    assert.equal(n, 2);
                    assert.equal(r.updatedExisting, true);
                    return coll.find({c: "multi"}).exec();
                }
            ).then(
                function (docs) {
                    assert(docs[0].j, docs[1].j);
                    _.each(docs, function (doc) {
                        assert.deepEqual(doc.a, []);
                    });
                    done();
                }
            ).end(done);
        });
        it('update with setting of _id field is not possible', function (done) {
            coll.update({c: "multi"}, {$set: {_id: "newId"}}, {multi: true}, function (err, res) {
                assert(_.isUndefined(res));
                assert(err);
                done();
            });
        });
    });

    describe("should insert", function () {
        it("works with String id", function (done) {
            coll.insert({_id: "some@email.goes.here.com", data: "some data"}, safe.sure(done, function () {
                coll.findOne({_id: "some@email.goes.here.com"}, safe.sure(done, function (obj) {
                    assert(obj);
                    done();
                }));
            }));
        });
        it("works with Date id", function (done) {
            var _id = new Date();
            coll.insert({_id: _id, data: "some data"}, safe.sure(done, function () {
                coll.findOne({_id: _id}, safe.sure(done, function (obj) {
                    assert(obj);
                    done();
                }));
            }));
        });
        it("works with Number id", function (done) {
            var _id = 1976;
            coll.insert({_id: _id, data: "some data"}, safe.sure(done, function () {
                coll.findOne({_id: _id}, safe.sure(done, function (obj) {
                    assert(obj);
                    done();
                }));
            }));
        });
    });

    describe("Distinct", function () {
        before(function (done) {
            coll.insert([
                { name: 'exec', age: 1 },
                { name: 'exec', age: 2 }
            ], done);
        });

        it("works", function (done) {
            coll.distinct('age', {name: 'exec'}, function (err, docs) {
                if (err) throw err;
                assert(2, docs.length);
                done();
            });
        });
    });
});
