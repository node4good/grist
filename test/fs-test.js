"use strict";
/*global it,describe */
var _ = require('lodash-contrib');
var assert = require('chai');
expect = assert.expect;
var loremIpsum = require('lorem-ipsum');
var DB = require('../');

var num = 100;
var gt0sin = 0;
var _dt = null;
global.CONN_STR_PREFIX = "grist-test-";

describe('FS', function () {
    describe("basic sanity", function () {
        it("can open db", function (done) {
            require.cache = {};
            var conn_str = global.CONN_STR_PREFIX + this.test.parent.title.replace(/\s/g, '');
            this._db = new DB(conn_str, {name: conn_str});
            done();
        });


        it("can fill the db", function (done) {
            var test = this;
            test.Dbname = "Basic-test";
            test._db.collection(test.Dbname, {}, function (err, _coll) {
                test.coll = _coll;
                test.coll.drop().then(function () {
                    gt0sin = 0;
                    _dt = null;
                    var objs = _.times(num, function (i) {
                        var d;
                        if (!_dt) _dt = d = new Date();
                        else d = new Date(_dt.getTime() + 1000 * i);
                        var obj = {
                            _dt: d,
                            dum: parseInt(i / 2),
                            num: i,
                            pum: i,
                            sub: {num: i},
                            sin: Math.sin(i),
                            cos: Math.cos(i),
                            t: 15,
                            junk: loremIpsum({count: 1, units: "paragraphs"})
                        };
                        obj.txt = obj.sin > 0 && "greater than zero" || obj.sin < 0 && "less than zero" || "zero";
                        if (obj.sin > 0 && obj.sin < 0.5)
                            gt0sin++;
                        return obj;
                    });
                    test.coll.insert(objs, done);
                });
            });
        });

        it("can drop the collections", function (done) {
            this._db.dropDatabase(done);
        });
    });

})
;
