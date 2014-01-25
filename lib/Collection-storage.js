var fs = require('fs');
var Collection = module.parent.exports;


Collection._storeFunc = function (self, pos, deleted, cb) {
    fs.open(self._filename, "a+", safe.sure(cb, function (fd) {
        self._fd = fd;
        var b1 = new Buffer(45);
        async.whilst(function () { return self._fsize === null; }, function (cb) {
            (function (cb) {
                fs.read(fd, b1, 0, 45, pos, safe.trap_sure(cb, function (bytes, data) {
                    if (bytes === 0) {
                        self._fsize = pos;
                        return cb();
                    }
                    var h1 = JSON.parse(data.toString());
                    h1.o = parseInt(h1.o, 10);
                    h1.k = parseInt(h1.k, 10);
                    var b2 = new Buffer(h1.k);
                    fs.read(fd, b2, 0, h1.k, pos + 45 + 1, safe.sure(cb, function (bytes, data) {
                        var k = JSON.parse(data.toString());
                        self._id = k._uid;
                        if (k._a == 'del') {
                            delete self._store[k._id];
                            deleted++;
                        } else {
                            if (self._store[k._id]) deleted++;
                            self._store[k._id] = { pos: pos, sum: k._s };
                        }
                        pos += 45 + 3 + h1.o + h1.k;
                        cb();
                    }));
                }));
            })(function (err) {
                if (err) cb(new Error(self._name + ": Error during load - " + err.toString()));
                else cb();
            });
        }, cb);
    }));
};


Collection._putWrite = function (self, sobj, key, item, buf, remove, cb) {
    var rec = self._store[key._id];
    if (rec && rec.sum == key._s) return cb();
    fs.write(self._fd, buf, 0, buf.length, self._fsize, function (err, written) {
        if (err) throw err;
        if (remove)
            delete self._store[key._id];
        else
            self._store[key._id] = { pos: self._fsize, sum: key._s };

        if (remove || sobj.length > self._cmaxobj)
            self._cache.unset(self._fsize);
        else
            self._cache.set(self._fsize, item);
        self._fsize += written;
        cb();
    });
};
