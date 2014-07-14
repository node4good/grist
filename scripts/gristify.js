var _ = require('lodash-contrib');
var fs = require('fs');
var readline = require('readline');

var filename = process.argv[2];

var store = {};
var fileStream = fs.createReadStream(filename);
var rl = readline.createInterface({
    input: fileStream,
    output: process.stdout
});
rl.on('line', function (line) {
    var raw = JSON.parse(line);
    var id = raw._id;
    if (_.isObject(raw._id))
        id = raw._id['$oid'];
    store[id] = sort(raw);
});
fileStream.on('end', function () {
    rl.close();
});
rl.on('close', function () {
    fs.writeFileSync(filename + '.json', JSON.stringify(store, null, '\t'));
    process.exit(0);
});


function sort(obj, options) {
    function sortBy(opts) {

        return function (objA, objB) {
            var result = objA < objB ? -1 : 1;
            if (opts.order.toLowerCase() === 'desc') {
                return result * -1;
            }
            return result;
        };
    }

    function sortKeys(obj, opts) {
        var keys = opts.keys(obj);
        keys.sort(sortBy(opts));
        return keys;
    }

    var opts = _.extend({ order: 'asc', property: false, keys: _.keys.bind(_) }, options);

    var sorted = {};
    var keys = [];
    var key;

    if (opts.property && opts.property !== false) {

        if (opts.property === true) {
            var inverted = _.invert(obj);
            keys = sortKeys(inverted, opts);

            for (var index in keys) {
                key = keys[index];
                sorted[inverted[key]] = key;
            }

        } else {

            var pairs = _.pairs(obj);
            var expanded = [];
            keys = {};
            for (var i = 0; i < pairs.length; i++) {
                key = pairs[i][1][opts.property];
                keys[key] = pairs[i][0];
                expanded.push(pairs[i][1]);
            }

            expanded = _.sortBy(expanded, opts.property);

            if (opts.order.toLowerCase() === 'desc') {
                expanded.reverse();
            }

            for (var i = 0; i < expanded.length; i++) {
                var value = expanded[i][opts.property];
                sorted[keys[value]] = expanded[i];
            }
        }

    } else {

        keys = sortKeys(obj, opts);
        for (var index in keys) {
            key = keys[index];
            sorted[key] = obj[key];
        }

    }

    return sorted;
}
