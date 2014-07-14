'use strict';
/*global describe */
var _ = require('lodash-contrib');
var assert = require('assert');
var tutils = require("./utils");
var Promise = require('mpromise');


describe('Misc', function () {
    var db, coll;
    before(function (done) {
        tutils.getDb('test', true, function (err, _db) {
            db = _db;
            db.collection('Misc', {}, function (err, _coll) {
                coll = _coll;
                coll.drop(done);
            });
        });
    });


    after(function (done) {
        coll.drop(done);
    });


    it('GH-20,GH-17 toArray should not fail/close with one record or count', function (done) {
        var cursor;
        coll.insert({}).then(
            function () {
                cursor = coll.find();
                var p = new Promise;
                cursor.count(p.resolve.bind(p));
                return p;
            }
        ).then(
            function (res) {
                assert.equal(res, 1);
                cursor.toArray(function () {
                    coll.drop(done);
                });
            }
        );
    });


    it('GH-14 Exclude projection for _id can be mixed with include projections', function (done) {
        coll.insert({name: 'Tony', age: '37'}).then(
            function () {
                return coll.findOne({}, {_id: 0, age: 1});
            }
        ).then(
            function (obj) {
                assert(!_.contains(_.keys(obj), '_id'));
                assert(_.contains(_.keys(obj), 'age'));
                assert(!_.contains(_.keys(obj), 'name'));
                return coll.findOne({}, {age: 1});
            }
        ).then(function (obj) {
                assert(_.contains(_.keys(obj), '_id'));
                assert(_.contains(_.keys(obj), 'age'));
                assert(!_.contains(_.keys(obj), 'name'));
                return coll.findOne({}, {age: 0});
            }
        ).then(function (obj) {
                assert(_.contains(_.keys(obj), '_id'));
                assert(!_.contains(_.keys(obj), 'age'));
                assert(_.contains(_.keys(obj), 'name'));
                return coll.findOne({}, {_id: 0, age: 0});
            }
        ).then(function (obj) {
                assert(!_.contains(_.keys(obj), '_id'));
                assert(!_.contains(_.keys(obj), 'age'));
                assert(_.contains(_.keys(obj), 'name'));
                return coll.findOne({}, {_id: 1, age: 0});
            }
        ).end(function (err) {
                assert(err);
                coll.drop(done);
            }
        );
    });
});
