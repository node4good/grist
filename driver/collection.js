var _ = require('lodash-contrib');
var Collection = require('../lib/Collection');
var MPromise = require('mpromise');
var util = require('util');


function PotusCollection(name, conn, opts) {
    if (undefined === opts) opts = {};
    if (undefined === opts.capped) opts.capped = {};

    opts.bufferCommands = undefined === opts.bufferCommands ? true : opts.bufferCommands;

    if ('number' == typeof opts.capped) {
        opts.capped = {size: opts.capped};
    }

    Collection.apply(this, [conn.db, name]);

    this.opts = opts;
    this.name = name;
    this.conn = conn;

    this.queue = [];
    this.buffer = this.opts.bufferCommands;

    if (conn.db)
        this.onOpen();
}
util.inherits(PotusCollection, Collection);


PotusCollection.prototype.onOpen = function () {
    var self = this;
    return this.init().then(function () {
        return self.conn.db.collection(self.name, function callback(err, collection) {
            if (err) throw err;
            self.collection = collection;
            self.buffer = false;
        });
    });
};

PotusCollection.prototype.ensureIndex = function (__, ___, cb) {
    return MPromise.fulfilled().onResolve(cb);
};

/**
 * Retreives information about this collections indexes.
 *
 * @param {Function} callback
 * @method getIndexes
 * @api public
 */

PotusCollection.prototype.getIndexes = Collection.prototype.indexInformation;

/*!
 * Module exports.
 */

module.exports = PotusCollection;
