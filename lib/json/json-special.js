'use strict';
var _ = require('lodash-contrib');
var ObjectId = require('../ObjectId');

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

function keyPath(object, keypath) {
    var current_object, i, key, keypath_components, l;
    keypath_components = keypath.split(".");
    if (keypath_components.length === 1) {
        return ((object instanceof Object) && (object.hasOwnProperty(keypath)) ? object[keypath] : void 0);
    }
    current_object = object;
    l = keypath_components.length;
    for (i in keypath_components) if (keypath_components.hasOwnProperty(i)) {
        key = keypath_components[i];
        if (!(key in current_object)) {
            break;
        }
        if (++i === l) {
            return current_object[key];
        }
        current_object = current_object[key];
        if (!current_object || (!(current_object instanceof Object))) {
            break;
        }
    }
    return void 0;
}


function tryParseType(json) {
    var type = json[JSONS.TYPE_FIELD];
    var _ref = JSONS.NAMESPACE_ROOTS;
    for (var _j = 0; _j < _ref.length; _j++) {
        var namespace_root = _ref[_j];
        var Constructor_or_root = keyPath(namespace_root, type);
        if (!Constructor_or_root) {
            continue;
        }
        if (Constructor_or_root.fromJSON) {
            return Constructor_or_root.fromJSON(json);
        } else if (Constructor_or_root.prototype && Constructor_or_root.prototype.parse) {
            var instance = new Constructor_or_root();
            if (instance.set) {
                return instance.set(instance.parse(json));
            }
            return instance.parse(json);
        }
    }
    return null;
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
    } else if (JSONS.TYPE_FIELD in json) {
        return tryParseType(json);
    } else {
        return Object.keys(json).reduce(function (seed, key) {
            seed[key] = JSONS.deserialize(json[key]);
            return seed;
        }, {});
    }
}
;

module.exports = JSONS;
