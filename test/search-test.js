"use strict";
var assert = require('assert');
var _ = require('lodash');
var safe = require('safe');
var lipsum = require('lorem-ipsum');
var tutils = require("./utils");
var Promise = require('mpromise');

var NUMBER_OF_DOCS = 1000;
var gt0sin = 0;
var _dt = null;

var words = ["Sergey Brin", "Serg Kosting", "Pupking Sergey", "Munking Sirgey"];

describe('Search', function () {
    this.timeout(60 * 60 * 1000);
    describe('New store', function () {
        var db, collection;
        before(function (done) {
            tutils.getDb('test', true, function (err, _db) {
                if (err) return done(err);
                db = _db;
                db.collection("test1", {}, function (err, _coll) {
                    if (err) return done(err);
                    collection = _coll;
                    collection.ensureIndex({num: 1}, {sparse: false, unique: false}, function (err, name) {
                        if (err) return done(err);
                        assert.ok(name);
                        var p = new Promise;
                        p.fulfill();
                        _.times(NUMBER_OF_DOCS, function (i) {
                            var timestamp = new Date();
                            if (_dt === null) _dt = timestamp;
                            var obj = {
                                _dt: timestamp,
                                anum: [i, i + 1, i + 2],
                                apum: [i, i + 1, i + 2],
                                num: i,
                                pum: i,
                                sub: { num: i },
                                sin: Math.sin(i),
                                cos: Math.cos(i),
                                t: 15,
                                junk: lipsum({ count: 5, units: "words" }) +
                                    words[i % words.length] +
                                    lipsum({ count: 5, units: "words" })
                            };
                            if (i % 7 === 0) {
                                obj.words = words;
                                delete obj.num;
                                delete obj.pum;
                            }
                            if (obj.sin > 0 && obj.sin < 0.5) gt0sin++;
                            p = p.then(function () {
                                return collection.insert(obj);
                            });
                        });
                        p.then(
                            function () {
                                done();
                            }
                        ).end();
                    });
                });
            });
        });


        it("Has right size", function (done) {
            collection.count(function (err, count) {
                assert.equal(count, NUMBER_OF_DOCS);
                done(err);
            });
        });
        it("find {num:10} (index)", function (done) {
            collection.find({num: 10}).toArray(function (err, docs) {
                assert.equal(docs.length, 1);
                assert.equal(docs[0].num, 10);
                done(err);
            });
        });
        it("find {num:{$not:{$ne:10}}} (index)", function (done) {
            collection.find({num: {$not: {$ne: 10}}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 1);
                assert.equal(docs[0].num, 10);
                done();
            }));
        });
        it("find {pum:10} (no index)", function (done) {
            collection.find({pum: 10}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 1);
                assert.equal(docs[0].pum, 10);
                done();
            }));
        });
        it("find {pum:{eq:10}} (no index)", function (done) {
            collection.find({num: 10}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 1);
                assert.equal(docs[0].pum, 10);
                done();
            }));
        });
        it("find {num:{$lt:10}} (index)", function (done) {
            collection.find({num: {$lt: 10}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 8);
                _.each(docs, function (doc) {
                    assert.ok(doc.num < 10);
                });
                done();
            }));
        });
        it("find {pum:{$lt:10}} (no index)", function (done) {
            collection.find({pum: {$lt: 10}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 8);
                _.each(docs, function (doc) {
                    assert.ok(doc.pum < 10);
                });
                done();
            }));
        });
        it("find {num:{$lte:10}} (index)", function (done) {
            collection.find({num: {$lte: 10}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 9);
                _.each(docs, function (doc) {
                    assert.ok(doc.num <= 10);
                });
                done();
            }));
        });
        it("find {pum:{$lte:10}} (no index)", function (done) {
            collection.find({pum: {$lte: 10}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 9);
                _.each(docs, function (doc) {
                    assert.ok(doc.pum <= 10);
                });
                done();
            }));
        });
        it("find {num:{$gt:10}} (index)", function (done) {
            collection.find({num: {$gt: 10}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 848);
                _.each(docs, function (doc) {
                    assert.ok(doc.num > 10);
                });
                done();
            }));
        });
        it("find {pum:{$gt:10}} (no index)", function (done) {
            collection.find({pum: {$gt: 10}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 848);
                _.each(docs, function (doc) {
                    assert.ok(doc.pum > 10);
                });
                done();
            }));
        });
        it("find {num:{$gte:10}} (index)", function (done) {
            collection.find({num: {$gte: 10}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 849);
                _.each(docs, function (doc) {
                    assert.ok(doc.num >= 10);
                });
                done();
            }));
        });
        it("find {pum:{$gte:10}} (no index)", function (done) {
            collection.find({pum: {$gte: 10}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 849);
                _.each(docs, function (doc) {
                    assert.ok(doc.pum >= 10);
                });
                done();
            }));
        });
        it("find {num:{$ne:10}} (index)", function (done) {
            collection.find({num: {$ne: 10}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, NUMBER_OF_DOCS - 1);
                _.each(docs, function (doc) {
                    assert.ok(doc.num != 10);
                });
                done();
            }));
        });
        it("find {num:{$not:{$eq:10}}} (index)", function (done) {
            collection.find({num: {$ne: 10}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, NUMBER_OF_DOCS - 1);
                _.each(docs, function (doc) {
                    assert.ok(doc.num != 10);
                });
                done();
            }));
        });
        it("find {pum:{$ne:10}} (no index)", function (done) {
            collection.find({pum: {$ne: 10}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, NUMBER_OF_DOCS - 1);
                _.each(docs, function (doc) {
                    assert.ok(doc.pum != 10);
                });
                done();
            }));
        });
        it("find {num:{$in:[10,20,30,40]}} (index)", function (done) {
            collection.find({num: {$in: [10, 20, 30, 40]}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 4);
                _.each(docs, function (doc) {
                    assert.ok(doc.num % 10 === 0);
                });
                done();
            }));
        });
        it("find {pum:{$in:[10,20,30,40]}} (no index)", function (done) {
            collection.find({pum: {$in: [10, 20, 30, 40]}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 4);
                _.each(docs, function (doc) {
                    assert.ok(doc.num % 10 === 0);
                });
                done();
            }));
        });
        it("find {num:{$nin:[10,20,30,40]}} (index)", function (done) {
            collection.find({num: {$nin: [10, 20, 30, 40]}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, NUMBER_OF_DOCS - 4);
                _.each(docs, function (doc) {
                    assert.ok(doc.num != 10 && doc.num != 20 && doc.num != 30 && doc.num != 40);
                });
                done();
            }));
        });
        it("find {pum:{$nin:[10,20,30,40]}} (no index)", function (done) {
            collection.find({pum: {$nin: [10, 20, 30, 40]}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, NUMBER_OF_DOCS - 4);
                _.each(docs, function (doc) {
                    assert.ok(doc.pum != 10 && doc.pum != 20 && doc.pum != 30 && doc.pum != 40);
                });
                done();
            }));
        });
        it("find {num:{$not:{$lt:10}}} (index)", function (done) {
            collection.find({num: {$not: {$lt: 10}}}).toArray(safe.sure(done, function (docs) {
                assert.ok(docs.length == 992 || docs.length == 850);	// Mongo BUG, 850 is wrong
                _.each(docs, function (doc) {
                    assert.ok(_.isUndefined(doc.num) || doc.num >= 10);
                });
                done();
            }));
        });
        it("find {pum:{$not:{$lt:10}}} (no index)", function (done) {
            collection.find({pum: {$not: {$lt: 10}}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 992);
                _.each(docs, function (doc) {
                    assert.ok(_.isUndefined(doc.pum) || doc.pum >= 10);
                });
                done();
            }));
        });
        it("find {num:{$lt:10},$or:[{num:5},{num:6},{num:11}]}", function (done) {
            collection.find({num: {$lt: 10}, $or: [
                {num: 5},
                {num: 6},
                {num: 11}
            ]}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 2);
                    _.each(docs, function (doc) {
                        assert.ok(doc.num < 10);
                    });
                    done();
                }));
        });
        it("find {num:{$lt:10},$nor:[{num:5},{num:6},{num:7}", function (done) {
            collection.find({num: {$lt: 10}, $nor: [
                {num: 5},
                {num: 6},
                {num: 7}
            ]}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 6);
                    _.each(docs, function (doc) {
                        assert.ok(doc.num < 10);
                    });
                    done();
                }));
        });
        it("find {'anum':{$all:[1,2,3]}} (index)", function (done) {
            collection.find({'anum': {$all: [1, 2, 3]}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 1);
                done();
            }));
        });
        it("find {'apum':{$all:[1,2,3]}} (no index)", function (done) {
            collection.find({'apum': {$all: [1, 2, 3]}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 1);
                done();
            }));
        });
        it("find {'pum':{$exists:false}} (no index)", function (done) {
            collection.find({'pum': {$exists: false}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 143);
                done();
            }));
        });
        it("find {'num':{$exists:false}} (index)", function (done) {
            collection.find({'num': {$exists: false}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 143);
                done();
            }));
        });
        it("find {'pum':{$exists:true}} (no index)", function (done) {
            collection.find({'pum': {$exists: true}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 857);
                done();
            }));
        });
        it("find {'num':{$exists:true}} (index)", function (done) {
            collection.find({'num': {$exists: true}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 857);
                done();
            }));
        });
        it("find {'junk':{$regex:'Sergey'}}", function (done) {
            collection.find({'junk': {$regex: 'Sergey'}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 500);
                done();
            }));
        });
        it("find {'junk':/Sergey/i}", function (done) {
            collection.find({'junk': /seRgey/i}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 500);
                done();
            }));
        });
        it("find {'junk':{$regex:'seRgey',$options:'i'}}", function (done) {
            collection.find({'junk': {$regex: 'seRgey', $options: 'i'}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 500);
                done();
            }));
        });
        it("find {'junk':{$options:'i',$regex:'seRgey'}}", function (done) {
            collection.find({'junk': {$options: 'i', $regex: 'seRgey'}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 500);
                done();
            }));
        });
        it("find {'junk':{$not:/sirgei/i}}", function (done) {
            collection.find({'junk': {$not: /sirgey/i}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 750);
                done();
            }));
        });
        it("find {'words':{$all:[/sirgey/i,/sergey/i]}}", function (done) {
            collection.find({'words': {$all: [/sirgey/i, /sergey/i]}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 143);
                done();
            }));
        });
    });
});
