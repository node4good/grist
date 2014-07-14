var _ = require('lodash-contrib');
var Collection = require('../lib/Collection-base');
var util = require('util');

function PotusCollection(name, conn, opts) {
    Collection.apply(this, arguments);

    if (undefined === opts) opts = {};
    if (undefined === opts.capped) opts.capped = {};

    opts.bufferCommands = undefined === opts.bufferCommands ? true : opts.bufferCommands;

    if ('number' == typeof opts.capped) {
        opts.capped = { size: opts.capped };
    }

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
    this.init(self.conn.db, self.name);
    return self.conn.db.collection(self.name, function callback(err, collection) {
        if (err) {
            // likely a strict mode error
            self.conn.emit('error', err);
        } else {
            self.collection = collection;
            self.buffer = false;
        }
    });
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
