function BinaryParserBuffer(bigEndian, buffer) {
    this.bigEndian = bigEndian || 0;
    this.buffer = [];
    this.setBuffer(buffer);
}


BinaryParserBuffer.prototype.hasNeededBits = function hasNeededBits(neededBits) {
    return this.buffer.length >= -(-neededBits >> 3);
};


BinaryParserBuffer.prototype.checkBuffer = function checkBuffer(neededBits) {
    if (!this.hasNeededBits(neededBits)) {
        throw new Error("checkBuffer::missing bytes");
    }
};


BinaryParserBuffer.prototype.setBuffer = function setBuffer(data) {
    var l, i, b;

    if (data) {
        i = l = data.length;
        b = this.buffer = new Array(l);
        for (; i; b[l - i] = data.charCodeAt(--i));
        if (this.bigEndian) b.reverse();
    }
};


BinaryParserBuffer.prototype.readBits = function readBits(start, length) {
    //shl fix: Henri Torgemane ~1996 (compressed by Jonas Raoni)

    function shl(a, b) {
        for (; b--; a = ((a %= 0x7fffffff + 1) & 0x40000000) == 0x40000000 ? a * 2 : (a - 0x40000000) * 2 + 0x7fffffff + 1);
        return a;
    }

    if (start < 0 || length <= 0) {
        return 0;
    }

    this.checkBuffer(start + length);

    var offsetLeft,
        offsetRight = start % 8,
        curByte = this.buffer.length - ( start >> 3 ) - 1,
        lastByte = this.buffer.length + ( -( start + length ) >> 3 ),
        diff = curByte - lastByte,
        sum = ((this.buffer[ curByte ] >> offsetRight) & ((1 << (diff ? 8 - offsetRight : length)) - 1)) + (diff && (offsetLeft = (start + length) % 8) ? (this.buffer[lastByte++] & ((1 << offsetLeft) - 1)) << (diff-- << 3) - offsetRight : 0);

    for (; diff; sum += shl(this.buffer[lastByte++], (diff-- << 3) - offsetRight));

    return sum;
};


var maxBits = [];
for (var i = 0; i < 64; i++) {
    maxBits[i] = Math.pow(2, i);
}
function encodeInt(data, bits, __, forceBigEndian) {
    var max = maxBits[bits];

    if (data >= max || data < -(max / 2)) {
        this.warn("encodeInt::overflow");
        data = 0;
    }

    if (data < 0) {
        data += max;
    }

    for (var r = []; data; r[r.length] = String.fromCharCode(data % 256), data = Math.floor(data / 256));

    for (bits = -(-bits >> 3) - r.length; bits--; r[r.length] = "\0");

    return ((this.bigEndian || forceBigEndian) ? r.reverse() : r).join("");
}

function decodeInt(data, bits, signed, forceBigEndian) {
    var b = new BinaryParserBuffer(this.bigEndian || forceBigEndian, data),
        x = b.readBits(0, bits),
        max = maxBits[bits];

    return (signed && x >= max / 2) ? x - max : x;
}


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
var ObjectID = function ObjectID(id) {
    if (!(this instanceof ObjectID)) return new ObjectID(id);

    // Throw an error if it's not a valid setup
    if (id && 'number' != typeof id && (id.length != 12 && id.length != 24))
        throw new Error("Argument passed in must be a single String of 12 bytes or a string of 24 hex characters");

    // Generate id based on the input
    if (!id || typeof id == 'number') {
        // convert to 12 byte binary string
        this.id = this.generate(id);
    } else if (id && id.length === 12) {
        // assume 12 byte string
        this.id = id;
    } else if (checkForHexRegExp.test(id)) {
        return ObjectID.createFromHexString(id);
    } else {
        throw new Error("Value passed in is not a valid 24 character hex string");
    }

    if (ObjectID.cacheHexString) this.__id = this.toHexString();
};


/**
 * Return the ObjectID id as a 24 byte hex string representation
 *
 * @return {String} return the 24 byte hex string representation.
 * @api public
 */
ObjectID.prototype.toHexString = function () {
    if (ObjectID.cacheHexString && this.__id) return this.__id;

    var hexString = '', number, value;

    for (var index = 0, len = this.id.length; index < len; index++) {
        value = decodeInt(this.id[index], 8, false);
        number = (value <= 15) ? '0' + value.toString(16) : value.toString(16);
        hexString = hexString + number;
    }

    if (ObjectID.cacheHexString) this.__id = hexString;
    return hexString;
};

/**
 * Update the ObjectID index used in generating new ObjectID's on the driver
 *
 * @return {Number} returns next index value.
 * @api private
 */
ObjectID.prototype.get_inc = function () {
    ObjectID.index = (ObjectID.index + 1) % 0xFFFFFF;
    return ObjectID.index;
};

/**
 * Update the ObjectID index used in generating new ObjectID's on the driver
 *
 * @return {Number} returns next index value.
 * @api private
 */
ObjectID.prototype.getInc = function () {
    return this.get_inc();
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
    var time4Bytes = 'number' == typeof time ? encodeInt(time, 32, true, true) : encodeInt(unixTime, 32, true, true);
    var machine3Bytes = encodeInt(MACHINE_ID, 24, false);
    var number = typeof process === 'undefined' ? Math.floor(Math.random() * 100000) : process.pid;
    var pid2Bytes = encodeInt(number, 16, true);
    var index3Bytes = encodeInt(this.get_inc(), 24, false, true);
    return time4Bytes + machine3Bytes + pid2Bytes + index3Bytes;
};

/**
 * Converts the id into a 24 byte hex string for printing
 *
 * @return {String} return the 24 byte hex string representation.
 * @api private
 */
ObjectID.prototype.toString = function () {
    return this.toHexString();
};

/**
 * Converts to a string representation of this Id.
 *
 * @return {String} return the 24 byte hex string representation.
 * @api private
 */
ObjectID.prototype.inspect = ObjectID.prototype.toString;

/**
 * Converts to its JSON representation.
 *
 * @return {String} return the 24 byte hex string representation.
 * @api private
 */
ObjectID.prototype.toJSON = function () {
    return this.toHexString();
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

/**
 * Returns the generation date (accurate up to the second) that this ID was generated.
 *
 * @return {Date} the generation date
 * @api public
 */
ObjectID.prototype.getTimestamp = function () {
    var timestamp = new Date();
    timestamp.setTime(Math.floor(decodeInt(this.id.substring(0, 4), 32, true, true)) * 1000);
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
    var id = encodeInt(time, 32, true, true) + encodeInt(0, 64, true, true);
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
    // Throw an error if it's not a valid setup
    if (typeof hexString === 'undefined' || hexString && hexString.length != 24)
        throw new Error("Argument passed in must be a single String of 12 bytes or a string of 24 hex characters");

    var len = hexString.length;

    if (len > 12 * 2) {
        throw new Error('Id cannot be longer than 12 bytes');
    }

    var result = '', string, number;

    for (var index = 0; index < len; index += 2) {
        string = hexString.substr(index, 2);
        number = parseInt(string, 16);
        result += encodeInt(number, 8, false);
    }

    return new ObjectID(result, hexString);
};

/**
 * @ignore
 */
Object.defineProperty(ObjectID.prototype, "generationTime", {
    enumerable: true, get: function () {
        return Math.floor(decodeInt(this.id.substring(0, 4), 32, true, true));
    }, set: function (value) {
        value = encodeInt(value, 32, true, true);
        this.id = value + this.id.substr(4);
        // delete this.__id;
        this.toHexString();
    }
});

/**
 * Expose.
 */
module.exports = ObjectID;
