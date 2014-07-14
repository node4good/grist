/*global it */
var assert = require('assert');
var _ = require('lodash-contrib');
var safe = require('safe');
var tutils = require("./utils");
var lipsum = require('lorem-ipsum');

var NUMBER_OF_DOCS = 1000;
var gt0sin = 0;
var _dt = null;

var words = ["Sergey Brin", "Serg Kosting", "Pupking Sergey", "Munking Sirgey"];

var num = 100;


describe('Search', function () {
    describe('Search Array', function () {
        var db, coll;
        before(function (done) {
            tutils.getDb('test', true, function (err, _db) {
                if (err) throw err;
                db = _db;
                db.collection("Search Array test1", {}, function (err, _coll) {
                    if (err) throw err;
                    coll = _coll;
                    coll.drop(function () {
                        var i = 1;
                        var objs = [];
                        while (i <= num) {
                            var arr = [], arr2 = [], j, obj;
                            for (j = i; j < i + 10; j++) {
                                obj = {num: j, pum: j, sub: {num: j, pum: j}};
                                if (i % 7 === 0) {
                                    delete obj.num;
                                    delete obj.pum;
                                }
                                arr.push(obj);
                                arr2.push(JSON.parse(JSON.stringify(obj)));
                            }
                            for (j = 0; j < 10; j++) {
                                arr[j].sub.arr = arr2;
                            }
                            obj = {num: i, pum: i, arr: arr, tags: ["tag" + i, "tag" + (i + 1)], nested: {tags: ["tag" + i, "tag" + (i + 1)]}};
                            objs.push(obj);
                            i++;
                        }
                        coll.insert(objs, done);
                    });
                });
            });
        });


        it("has proper size", function (done) {
            coll.count(safe.sure(done, function (size) {
                assert.equal(size, num);
                done();
            }));
        });


        it("find {'arr.num':10} (index)", function (done) {
            coll.find({'arr.num': 10}, {"_tiar.arr.num": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 9);
                _.each(docs, function (doc) {
                    var found = false;
                    _.each(doc.arr, function (obj) {
                        if (obj.num == 10)
                            found = true;
                    });
                    assert.ok(found);
                });
                done();
            }));
        });

        it("find {'arr.pum':10} (no index)", function (done) {
            coll.find({'arr.pum': 10}, {"_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 9);
                _.each(docs, function (doc) {
                    var found = false;
                    _.each(doc.arr, function (obj) {
                        if (obj.pum == 10)
                            found = true;
                    });
                    assert.ok(found);
                });
                done();
            }));
        });

        describe.skip("failing", function () {
            it("find {'arr.num':{$ne:10}} (index)", function (done) {
                coll.find({'arr.num': {$ne: 10}}, {"_tiar.arr.num": 0}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 86);
                    var found = false;
                    _.each(docs, function (doc) {
                        _.each(doc.arr, function (obj) {
                            if (obj.num == 10)
                                found = true;
                        });
                    });
                    assert.ok(found);
                    done();
                }));
            });

            it("find {'arr.pum':{$ne:10}} (no index)", function (done) {
                coll.find({'arr.pum': {$ne: 10}}, {"_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 91);
                    _.each(docs, function (doc) {
                        var found = false;
                        _.each(doc.arr, function (obj) {
                            if (obj.pum == 10)
                                found = true;
                        });
                        assert.ok(!found);
                    });
                    done();
                }));
            });

            it("find {'arr.num':{$nin:[10,20,30,40]}} (index)", function (done) {
                coll.find({'arr.num': {$nin: [10, 20, 30, 40]}}, {"_tiar.arr.num": 0}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 65);
                    done();
                }));
            });

            it("find {'arr.pum':{$nin:[10,20,30,40]}} (no index)", function (done) {
                coll.find({'arr.pum': {$nin: [10, 20, 30, 40]}}, {"_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 65);
                    done();
                }));
            });

            it("find {'arr.num':{$all:[1,2,3,4,5,6,7,8,9,10]}} (index)", function (done) {
                coll.find({'arr.num': {$all: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}}, {"_tiar.arr.num": 0}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 1);
                    done();
                }));
            });

            it("find {'arr.pum':{$all:[1,2,3,4,5,6,7,8,9,10]}} (no index)", function (done) {
                coll.find({'arr.pum': {$all: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}}, {"_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 1);
                    done();
                }));
            });

            it("find {'arr.pum':{$exists:false}} (no index)", function (done) {
                coll.find({'arr.pum': {$exists: false}}, {"_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 14);
                    done();
                }));
            });

            it("find {'arr.num':{$exists:false}} (index)", function (done) {
                coll.find({'arr.num': {$exists: false}}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 14);
                    done();
                }));
            });

        });


        it("find {'arr.num':{$gt:10}} (index)", function (done) {
            coll.find({'arr.num': {$gt: 10}}, {"_tiar.arr.num": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 85);
                done();
            }));
        });

        it("find {'arr.pum':{$gt:10}} (no index)", function (done) {
            coll.find({'arr.pum': {$gt: 10}}, {"_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 85);
                done();
            }));
        });

        it("find {'arr.num':{$gte:10}} (index)", function (done) {
            coll.find({'arr.num': {$gte: 10}}, {"_tiar.arr.num": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 86);
                done();
            }));
        });

        it("find {'arr.pum':{$gte:10}} (no index)", function (done) {
            coll.find({'arr.pum': {$gte: 10}}, {"_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 86);
                done();
            }));
        });

        it("find {'arr.num':{$lt:10}} (index)", function (done) {
            coll.find({'arr.num': {$lt: 10}}, {"_tiar.arr.num": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 8);
                done();
            }));
        });

        it("find {'arr.pum':{$lt:10}} (no index)", function (done) {
            coll.find({'arr.pum': {$lt: 10}}, {"_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 8);
                done();
            }));
        });

        it("find {'arr.num':{$lte:10}} (index)", function (done) {
            coll.find({'arr.num': {$lte: 10}}, {"_tiar.arr.num": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 9);
                done();
            }));
        });

        it("find {'arr.pum':{$lte:10}} (no index)", function (done) {
            coll.find({'arr.pum': {$lte: 10}}, {"_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 9);
                done();
            }));
        });

        it("find {'arr.num':{$in:[10,20,30,40]}} (index)", function (done) {
            coll.find({'arr.num': {$in: [10, 20, 30, 40]}}, {"_tiar.arr.num": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 35);
                done();
            }));
        });

        it("find {'arr.pum':{$in:[10,20,30,40]}} (no index)", function (done) {
            coll.find({'arr.pum': {$in: [10, 20, 30, 40]}}, {"_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 35);
                done();
            }));
        });

        it("find {'arr.num':{$lt:10},$or:[{'arr.pum':3},{'arr.pum':5},{'arr.pum':7}]}", function (done) {
            coll.find(
                {
                    'arr.num': {$lt: 10},
                    $or: [
                        {'arr.pum': 3},
                        {'arr.pum': 5},
                        {'arr.pum': 7}
                    ]
                },
                {
                    "_tiar.arr.num": 0,
                    "_tiar.arr.pum": 0
                }
            ).toArray(
                function (err, docs) {
                    if (err) throw err;
                    assert.equal(docs.length, 6);
                    done();
                }
            );
        });

        it("find {'arr.pum':{$lt:10},$or:[{'arr.num':3},{'arr.num':5},{'arr.num':7}]}", function (done) {
            coll.find({'arr.pum': {$lt: 10}, $or: [
                    {'arr.num': 3},
                    {'arr.num': 5},
                    {'arr.num': 7}
                ]},
                {"_tiar.arr.num": 0, "_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 6);
                    done();
                }));
        });

        it("find {'arr.num':{$lt:10},$nor:[{'arr.pum':3},{'arr.pum':5},{'arr.pum':7}]}", function (done) {
            coll.find({'arr.num': {$lt: 10}, $nor: [
                    {'arr.pum': 3},
                    {'arr.pum': 5},
                    {'arr.pum': 7}
                ]},
                {"_tiar.arr.num": 0, "_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 2);
                    done();
                }));
        });

        it("find {'arr.pum':{$lt:10},$nor:[{'arr.num':3},{'arr.num':5},{'arr.num':7}]}", function (done) {
            coll.find({'arr.pum': {$lt: 10}, $nor: [
                    {'arr.num': 3},
                    {'arr.num': 5},
                    {'arr.num': 7}
                ]},
                {"_tiar.arr.num": 0, "_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 2);
                    done();
                }));
        });

        it("find {'arr.pum':{$exists:true}} (no index)", function (done) {
            coll.find({'arr.pum': {$exists: true}}, {"_tiar.arr.pum": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 86);
                done();
            }));
        });

        it("find {'arr.num':{$exists:true}} (index)", function (done) {
            coll.find({'arr.num': {$exists: true}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 86);
                done();
            }));
        });

        it("find flat array {'tags':'tag2'} (no index)", function (done) {
            coll.find({'tags': 'tag2'}, {"_tiar.tags": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 2);
                done();
            }));
        });
        it("find nested flat array {'nested.tags':'tag2'} (no index)", function (done) {
            coll.find({'nested.tags': 'tag2'}, {"_tiar.nested.tags": 0}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 2);
                done();
            }));
        });

        it("find flat array {'tags':'tag2'} (index)", function (done) {
            coll.find({'tags': 'tag2'}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 2);
                done();
            }));
        });

        it("find nested flat array {'nested.tags':'tag2'} (index)", function (done) {
            coll.find({'nested.tags': 'tag2'}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 2);
                done();
            }));
        });
    });


    describe('Search General', function () {
        this.timeout(60 * 60 * 1000);
        var db, collection;
        before(function (done) {
            tutils.getDb('test', true, function (err, _db) {
                if (err) return done(err);
                db = _db;
                db.collection("Search-test", {}, function (err, _coll) {
                    if (err) return done(err);
                    collection = _coll;
                    collection.drop(function (err) {
                        if (err) return done(err);
                        var objs = [];
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
                            objs.push(obj);
                        });
                        collection.insert(objs, done);
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

        it("find {'junk':{$not:/sirgei/i}}", function (done) {
            collection.find({'junk': {$not: /sirgey/i}}).toArray(safe.sure(done, function (docs) {
                assert.equal(docs.length, 750);
                done();
            }));
        });

        describe.skip("failing", function () {
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
            it("find {'words':{$all:[/sirgey/i,/sergey/i]}}", function (done) {
                collection.find({'words': {$all: [/sirgey/i, /sergey/i]}}).toArray(safe.sure(done, function (docs) {
                    assert.equal(docs.length, 143);
                    done();
                }));
            });
        });
    });
})
;
