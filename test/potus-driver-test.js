"use strict";
/*global it,describe */
var _ = require('lodash-contrib');
var assert = require('chai').assert;
var safe = require('safe');
var loremIpsum = require('lorem-ipsum');
require('./driver/common').getMongoose();


var num = 100;
var gt0sin = 0;
var _dt = null;


describe('Potus', function () {
    before(function (done) {
        this.timeout(10000);
        var Team = this.coll = require('./fixtures/team.js');
        this.coll.collection.drop().then(function () {
            gt0sin = 0;
            _dt = null;
            var objs = _.times(num, function (i) {
                var d;
                if (!_dt) _dt = d = new Date();
                else d = new Date(_dt.getTime() + 1000 * i);
                var obj = {
                    createdAt: d,
                    name: String(Math.sin(i)),
                    location: loremIpsum({count: 1, units: "paragraphs"})
                };
                obj.txt = obj.sin > 0 && "greater than zero" || obj.sin < 0 && "less than zero" || "zero";
                if (obj.sin > 0 && obj.sin < 0.5)
                    gt0sin++;
                return new Team(obj);
            });
            Team.create(objs, done);
        }).catch();
    });


    describe('New store', function () {
        it("Has right size", function (done) {
            this.coll.count(function (err, count) {
                assert.equal(count, num);
                done(err);
            });
        });
    });


    describe('Existing store', function () {
        var coll;
        before(function (done) {
            coll = require('./fixtures/team.js');
            done();
        });

        it("Collection.count", function (done) {
            coll.count(function (err, count) {
                assert.equal(count, num);
                done(err);
            });
        });
        it("utf8 text", function (done) {
            var x = coll.find({name: {$gt: 0}});
            x.exec(function (err, docs) {
                if (err) throw err;
                assert(!_.isEmpty(docs));
                done();
            });
        });
    });
})
;
