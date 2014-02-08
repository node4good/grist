'use strict';
/*global describe */
var assert = require('assert');
var _ = require('lodash');
var safe = require('safe');
var tutils = require("./utils");


describe('Misc', function () {
    var db, coll;
    before(function (done) {
        tutils.getDb('test', true, safe.sure(done, function (_db) {
            db = _db;
            db.collection('Misc', {}, safe.sure(done, function (_coll) {
                coll = _coll;
                coll.drop(done);
            }));
        }));
    });


    after(function (done) {
        coll.drop(done);
    });


    it('GH-20,GH-17 toArray should not fail/close with one record or count', function (done) {
        coll.insert({}, safe.sure(done, function () {
            var cursor = coll.find();
            cursor.count(safe.sure(done, function (res) {
                assert.equal(res, 1);
                cursor.toArray(function () {
                    coll.drop(done);
                });
            }));
        }));
    });


    it('GH-14 Exclude projection for _id can be mixed with include projections', function (done) {
        coll.insert({name: 'Tony', age: '37'}, safe.sure(done, function () {
            coll.findOne({}, {_id: 0, age: 1}, safe.sure(done, function (obj) {
                assert(!_.contains(_.keys(obj), '_id'));
                assert(_.contains(_.keys(obj), 'age'));
                assert(!_.contains(_.keys(obj), 'name'));
                coll.findOne({}, {age: 1}, safe.sure(done, function (obj) {
                    assert(_.contains(_.keys(obj), '_id'));
                    assert(_.contains(_.keys(obj), 'age'));
                    assert(!_.contains(_.keys(obj), 'name'));
                    coll.findOne({}, {age: 0}, safe.sure(done, function (obj) {
                        assert(_.contains(_.keys(obj), '_id'));
                        assert(!_.contains(_.keys(obj), 'age'));
                        assert(_.contains(_.keys(obj), 'name'));
                        coll.findOne({}, {_id: 0, age: 0}, safe.sure(done, function (obj) {
                            assert(!_.contains(_.keys(obj), '_id'));
                            assert(!_.contains(_.keys(obj), 'age'));
                            assert(_.contains(_.keys(obj), 'name'));
                            coll.findOne({}, {_id: 1, age: 0}, function (err) {
                                assert(err);
                                coll.drop(done);
                            });
                        }));
                    }));
                }));
            }));
        }));
    });
});
