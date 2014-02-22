/**
 * Machine id.
 *
 * Create a random 3-byte value (i.e. unique for this
 * process). Other drivers use a md5 of the machine id here, but
 * that would mean an asyc call to gethostname, so we don't bother.
 */
var MACHINE_ID = parseInt(Math.random() * 0xFFFFFF, 10);

// Regular expression that checks for hex value
var checkForHexRegExp = new RegExp("^[0-9a-fA-F]{24}$");

/**
 * Create a new ObjectID instance
 *
 * @class Represents the BSON ObjectID type
 * @param {String|Number} id Can be a 24 byte hex string, 12 byte binary string or a Number.
 * @return {Object} instance of ObjectID.
 */
function ObjectID(id) {
    if (!(this instanceof ObjectID)) return new ObjectID(id);

    // Throw an error if it's not a valid setup
    if (id && 'number' != typeof id && id.length != 24)
        throw new Error("Argument passed in must be a single String of of 24 hex characters");

    // Generate id based on the input
    if (!id || typeof id == 'number') {
        // convert to 12 byte binary string
        this.id = this.generate(id);
    } else if (id && checkForHexRegExp.test(id)) {
        this.id = id;
    } else {
        throw new Error("Value passed in is not a valid 24 character hex string: ", id);
    }
}


ObjectID.tryToParse = function tryToParse(val) {
    if (val instanceof Object && '$oid' in val)
        return new ObjectID(val['$oid']);
    if (val && checkForHexRegExp.test(val))
        return new ObjectID(val);
    else
        return val;
};


/**
 * Return the ObjectID id as a 24 byte hex string representation
 *
 * @return {String} return the 24 byte hex string representation.
 * @api public
 */
ObjectID.prototype.toHexString = function () {
    return this.id;
};


function toHex(num, byteSize) {
    var charSize = byteSize * 2;
    var str = num.toString(16).slice(0, charSize);
    var padLength = charSize - str.length;
    var pad = new Array(padLength + 1).join('0');
    return pad + str;
}


//noinspection JSUnusedGlobalSymbols
/**
 * Update the ObjectID index used in generating new ObjectID's on the driver
 *
 * @return {Number} returns next index value.
 * @api private
 */
ObjectID.prototype.getInc = ObjectID.prototype.get_inc = function () {
    ObjectID.index = (ObjectID.index + 1) % 0xFFFFFF;
    return ObjectID.index;
};


/**
 * Generate a 12 byte id string used in ObjectID's
 *
 * @param {Number} [time] optional parameter allowing to pass in a second based timestamp.
 * @return {String} return the 12 byte id binary string.
 * @api private
 */
ObjectID.prototype.generate = function (time) {
    var unixTime = parseInt(Date.now() / 1000, 10);
    var timeStamp = 'number' == typeof time ? time : unixTime;
    var number = typeof process === 'undefined' ? Math.floor(Math.random() * 100000) : process.pid;

    var time4Bytes = toHex(timeStamp, 4);
    var machine3Bytes = toHex(MACHINE_ID, 3);
    var pid2Bytes = toHex(number, 2);
    var index3Bytes = toHex(this.get_inc(), 3);
    return time4Bytes + machine3Bytes + pid2Bytes + index3Bytes;
};

/**
 * Converts the id into a 24 byte hex string for printing
 *
 * @return {String} return the 24 byte hex string representation.
 * @api private
 */
ObjectID.prototype.inspect = ObjectID.prototype.toString = function () {
    return this.toHexString();
};

/**
 * Converts to its JSON representation.
 *
 * @return {String} return the 24 byte hex string representation.
 * @api private
 */
ObjectID.prototype.toJSON = function () {
    return {'$oid': this.toHexString()};
};

/**
 * Compares the equality of this ObjectID with `otherID`.
 *
 * @param {Object} otherID ObjectID instance to compare against.
 * @return {Boolean} the result of comparing two ObjectID's
 * @api public
 */
ObjectID.prototype.equals = function equals(otherID) {
    var id = (otherID instanceof ObjectID || otherID.toHexString) ? otherID.id : ObjectID.createFromHexString(otherID).id;
    return this.id === id;
};

//noinspection JSUnusedGlobalSymbols
/**
 * Returns the generation date (accurate up to the second) that this ID was generated.
 *
 * @return {Date} the generation date
 * @api public
 */
ObjectID.prototype.getTimestamp = function () {
    var timestamp = new Date();
    var ticks = parseInt(this.id.substring(0, 8), 16) * 1000;
    timestamp.setTime(ticks);
    return timestamp;
};

/**
 * @ignore
 * @api private
 */
ObjectID.index = 0;

ObjectID.createPk = function createPk() {
    return new ObjectID();
};

/**
 * Creates an ObjectID from a second based number, with the rest of the ObjectID zeroed out. Used for comparisons or sorting the ObjectID.
 *
 * @param {Number} time an integer number representing a number of seconds.
 * @return {ObjectID} return the created ObjectID
 * @api public
 */
ObjectID.createFromTime = function createFromTime(time) {
    var id = toHex(time, 4) + toHex(0, 8);
    return new ObjectID(id);
};

/**
 * Creates an ObjectID from a hex string representation of an ObjectID.
 *
 * @param {String} hexString create a ObjectID from a passed in 24 byte hexstring.
 * @return {ObjectID} return the created ObjectID
 * @api public
 */
ObjectID.createFromHexString = function createFromHexString(hexString) {
    return new ObjectID(hexString);
};


/**
 * @ignore
 */
Object.defineProperty(ObjectID.prototype, "generationTime", {
    enumerable: true, get: function () {
        return parseInt(this.id.substring(0, 8), 16);
    }, set: function (value) {
        value = toHex(value, 4);
        this.id = value + this.id.substr(4);
        this.toHexString();
    }
});

/**
 * Expose.
 */
module.exports = ObjectID;
