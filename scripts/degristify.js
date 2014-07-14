var _ = require('lodash-contrib');
var fs = require('fs');

var filename = process.argv[2];

var file = fs.readFileSync(filename);
var store = JSON.parse(file);
var string = _.values(store).map(function (item) {
    return JSON.stringify(item);
}).join('\n');
fs.writeFileSync(filename + '.2.json', string);
