var _ = require('lodash');

module.exports.intersectIndexes = function (indexes, base) {
    // do intersection of indexes using hashes
    var ops = [];
    // convert to hashes
    indexes.forEach(function (index) {
        var ids = {};
        _.each(index, function (id) {
            ids[id] = id;
        });
        ops.push(ids);
    });
    // find minimal one
    if (_.isUndefined(base)) {
        base = 0;
        for (var j = 0; j < ops.length; j++) {
            if (ops[j].length < ops[base].length)
                base = j;
        }
    }
    // iterate over it
    var m = [];
    _.each(indexes[base], function (id) {
        var match = true;
        for (var i = 0; i < ops.length; i++) {
            if (i == base) continue;
            if (!ops[i][id]) {
                match = false;
                break;
            }
        }
        if (match)
            m.push(id);
    });
    return m;
};
