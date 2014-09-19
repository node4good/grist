"use strict";
var path = require('path');
var BSONUtils = require('../scripts/BSONUtils');
var tutils = require("./utils");


describe('BSON Parsing', function () {
    before(function (done) {
        var test = this;
        tutils.getDb('test', true, function (err, _db) {
            test._db = _db;
            _db.dropDatabase(done);
        });
    });


    it("Read Dir", function (done) {
        this.timeout(10 * 1000);
        var fixtureDir = path.join(__dirname, 'sample-data');
        BSONUtils.BSONDirectoryConvert(fixtureDir, this._db, done);
    });
});
