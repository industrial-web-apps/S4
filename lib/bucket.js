var sqlite3 = require('sqlite3').verbose(),
    crypto = require('crypto'),
    async = require('async'),
    storage = require('filestorage').create('./files'),
    fs = require('fs'),
    PassThrough = require('stream').PassThrough;
    _ = require('lodash');

function Bucket(name) {
    var self = this;
    self.name = name;
    self._db = new sqlite3.Database('dbs/bucket_' + name + '.db');

    self._db.serialize(function () {
        var commands = [
            'CREATE TABLE files (name TEXT)'
        ];
        async.each(commands, self._db.run.bind(self._db), setReady.bind(self));
    });
}

_.extend(Bucket.prototype, {
    _ready: false,
    _readyFuncs: [],
    onReady: function (cb) {
        if (!cb)
            return;
        if (this._ready) {
            cb.apply(this);
        } else {
            this._readyFuncs.push(cb.bind(this));
        }
    },
    insertFile: function (key, fileStream, cb) {
        fileStream = typeof fileStream === 'string' ? fs.createReadStream(fileStream) : fileStream;
        var hash = crypto.createHash('md5'),
            md5PassThrough = new PassThrough();
        hash.setEncoding('hex');

        fileStream.pipe(hash);
        fileStream.pipe(md5PassThrough);

        fileStream.on('end', function () {
            hash.end();
            storage.insert(key, md5PassThrough, { md5: hash.read() }, cb);
        });
    },
    getFile: function (key, cb) {
        storage.read(key, cb);
    },
    getFileStream: function (key, stream) {
        storage.pipe(key, stream);
    },
    deleteFile: function (key, cb) {
        storage.remove(key, cb);
    }
});

function setReady() {
    var self = this;
    self._ready = true;
    self._readyFuncs.forEach(function (cb) { cb && cb(null, self); });
    self._readyFuncs = null;
}

module.exports = {
    init: function (name, cb) {
        var bucket = new Bucket(name);
        bucket.onReady(cb);
        return bucket;
    }
};