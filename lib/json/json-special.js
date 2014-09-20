'use strict';
var _ = require('lodash-contrib');
var ObjectId = require('./ObjectId');

var JSONS = {
    TYPE_FIELD: "_type",
    OBJECT_ID_FIELD: "$oid",
    ID_FIELD: "_id"
};

function stringHasISO8601DateSignature(string) {
    return _.isString(string) && _.all([
        string.length >= 19,
        string[4] === "-",
        string[7] === "-",
        string[10] === "T",
        string[string.length - 1] === "Z"
    ]);
}

JSONS.deserialize = function (json) {
    if (_.isEmpty(json)) return json;
    if (json instanceof ObjectId) return json;
    if (_.isString(json)) {
        if (stringHasISO8601DateSignature(json))
            return new Date(json);
        else
            return json;
    }

    if (JSONS.ID_FIELD in json) {
        var raw_id = json._id;
        if (raw_id.toString().length == 24 || _.has(raw_id, '$oid'))
            json._id = ObjectId.tryToParse(raw_id);
    }

    if (_.isArray(json)) {
        return json.map(JSONS.deserialize);
    } else if (JSONS.OBJECT_ID_FIELD in json) {
        return ObjectId.tryToParse(json);
    } else {
        return Object.keys(json).reduce(function (seed, key) {
            seed[key] = JSONS.deserialize(json[key]);
            return seed;
        }, {});
    }
}
;

module.exports = JSONS;
