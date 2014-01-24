function tdb() {}

tdb.prototype.init = function init(path, options, cb) {
    this._path = path;
    cb(null);
};


module.exports = tdb;

