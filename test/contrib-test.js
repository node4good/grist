/*global console,afterEach */
var tutils = require('./utils');

var config = function (options) {
    return function () {
        var self = this;
        options = options || {};
        var db = tutils.getDbSync('test', { w: 0, native_parser: false }, { auto_reconnect: false, poolSize: 4 }, true);

        // Test suite start
        self.start = function (callback) {
            tutils.openEmpty(db, callback);
        };

        self.restart = function (callback) {
            self.stop(function (err) {
                if (err) callback(err);
                else self.start(callback);
            });
        };

        // Test suite stop
        self.stop = function (callback) {
            db.close(callback);
        };

        // Pr test functions
        self.setup = function (callback) { callback(); };
        self.teardown = function (callback) { callback(); };

        // Returns the package for using Mongo driver classes
        self.getMongoPackage = function () {
            return require('mongodb');
        };

        self.newDbInstance = function (db_options, server_options) {
            return tutils.getDbSync("test", db_options, server_options, true);
        };

        // Returns a db
        self.db = function () {
            return db;
        };

        self.url = function (user, password) {
            if (user) {
                return 'mongodb://' + user + ':' + password + '@localhost:27017/' + self.db_name + '?safe=false';
            }

            return 'mongodb://localhost:27017/' + self.db_name + '?safe=false';
        };

        // Used in tests
        self.db_name = "test";
    };
};


var assert = require('assert');
var _ = require('lodash');

var dir = './contrib';
var files = [
    'collection_tests',
    'cursor_tests',
    'cursorstream_tests',
    'find_tests',
    'insert_tests',
    'mapreduce_tests',
    'remove_tests'
];

var slow = {
    'shouldStreamDocumentsWithPauseAndResumeForFetching': 10000,
    'shouldNotFailDueToStackOverflowEach': 30000,
    'shouldNotFailDueToStackOverflowToArray': 30000,
    'shouldStream10KDocuments': 60000
};

describe.none = function () {};
describe.none('contrib', function () {
    var names;
    var configuration;
    this.timeout(10000);
    before(function (done) {
        names = {};
        configuration = new (config())();
        configuration.start(done);
    });
    _(files).each(function (file) {
        var tests = require(dir + '/' + file);
        describe(file, function () {
            _(tests).each(function (fn, name) {
                if (typeof fn != 'function') return;
                describe(name, function () {
                    var done;
                    var test = this;
                    if (slow[name]) test.timeout(slow[name]);
                    it('test', function (_done) {
                        done = _done;
                        if (names[name]) {
                            console.log('dup: ' + name);
                            return done();
                        }
                        names[name] = true;
                        var test = {
                            ok: function (x) { assert.ok(x); },
                            equal: function (x, y) { assert.equal(y, x); },
                            deepEqual: function (x, y) { assert.deepEqual(y, x); },
                            throws: function (x, y) { assert.throws(x, y); },
                            done: function () { done(); }
                        };
                        fn(configuration, test);
                    });
                    afterEach(function () {
                        done = function () {};
                    });
                });
            });
        });
    });
    after(function (done) {
        configuration.stop(done);
    });
});
