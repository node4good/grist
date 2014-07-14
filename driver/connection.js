var fs = require('fs'),
    util = require('util'),
    EventEmitter = require('events').EventEmitter,
    _ = require('lodash-contrib'),
    safe = require('safe'),
    PotusCollection = require('./collection.js'),
    Db = require('./');

function Connection(base) {
    this.base = base;
    this.collections = {};
    this.models = {};
    this.db = null;
}
util.inherits(Connection, EventEmitter);


Connection.prototype.collection = function (name, options) {
    if (!(name in this.collections))
        this.collections[name] = new PotusCollection(name, this, options);
    return this.collections[name];
};

var dbs = {};

Connection.prototype.open = function (uri, cb) {
    var self = this;
    cb = _.isFunction(cb) ? cb : _.noop;

    var path = uri.match("grist://(.*)");
    if (!path)
        throw new Error("grist supports connection string format 'grist://{/path/to/valid/local/folder'");
    path = path[1];
    if (!fs.existsSync(path)) fs.mkdirSync(path);

    this.emit("connecting");

    if (!dbs[path]) {
        if (!fs.existsSync(path))
            fs.mkdirSync(path);
        dbs[path] = new Db(path, {});
    }

    this.emit("connected");

    this.db = dbs[path];

    _.forEach(this.collections, function (coll) {
        coll.onOpen();
    });

    process.nextTick(function () {
        cb();
        self.emit('open');
    });

    return this;
};

Connection.prototype.close = function () {
    var self = this;
    var cb = arguments[arguments.length - 1];
    cb = _.isFunction(cb) ? cb : _.noop;

    safe.yield(function () {
        cb();
        self.emit('close');
    });
    return this;
};

Connection.prototype.model = function (name, schema, collection) {
    var Schema = this.base.Schema;
    var MongooseError = this.base.Error;

    // collection name discovery
    if ('string' == typeof schema) {
        collection = schema;
        schema = false;
    }

    if (_.isObject(schema) && !(schema instanceof Schema)) {
        schema = new Schema(schema);
    }

    if (this.models[name] && !collection) {
        // model exists but we are not subclassing with custom collection
        if (schema instanceof Schema && schema != this.models[name].schema) {
            throw new MongooseError.OverwriteModelError(name);
        }
        return this.models[name];
    }

    var opts = { cache: false, connection: this };
    var model;

    if (schema instanceof Schema) {
        // compile a model
        model = this.base.model(name, schema, collection, opts);

        // only the first model with this name is cached to allow
        // for one-offs with custom collection names etc.
        if (!this.models[name]) {
            this.models[name] = model;
        }

        model.init();
        return model;
    }

    if (this.models[name] && collection) {
        // subclassing current model with alternate collection
        model = this.models[name];
        schema = model.prototype.schema;
        var sub = model.__subclass(this, schema, collection);
        // do not cache the sub model
        return sub;
    }

    // lookup model in mongoose module
    model = this.base.models[name];

    if (!model) {
        throw new MongooseError.MissingSchemaError(name);
    }

    if (this == model.prototype.db && (!collection || collection == model.collection.name)) {
        // model already uses this connection.

        // only the first model with this name is cached to allow
        // for one-offs with custom collection names etc.
        if (!this.models[name]) {
            this.models[name] = model;
        }

        return model;
    }

    var ret = this.models[name] = model.__subclass(this, schema, collection);
    return ret;
};


Connection.prototype.modelNames = function () {
    return _.keys(this.models);
};


module.exports = Connection;
