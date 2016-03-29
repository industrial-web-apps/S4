var sqlite3 = require('sqlite3').verbose(),
    crypto = require('crypto'),
    async = require('async'),
    storage = require('filestorage'),
    fs = require('fs-extra'),
    _ = require('lodash');

function Bucket(name) {
    var self = this;
    self.name = name;
    self._db = new sqlite3.Database('dbs/bucket_' + name + '.db');
    self._storage = storage.create('./files/' + name);
    self._ready = false;
    self._readyFuncs = [];

    self._db.serialize(function () {
        var commands = [
            'CREATE TABLE IF NOT EXISTS files (key TEXT, fileId INTEGER);',
            'CREATE UNIQUE INDEX IF NOT EXISTS key_idx on files (key);',
            'CREATE UNIQUE INDEX IF NOT EXISTS fileId_idx on files (fileId);'
        ];
        async.each(commands, self._db.run.bind(self._db), self._setReady.bind(self));
    });
}

_.extend(Bucket.prototype, {
    _setReady: function () {
        var self = this;
        self._ready = true;
        self._readyFuncs.forEach(function (cb) { cb && cb(null, self); });
        self._readyFuncs = null;
    },
    _getFileIdByKey: function (key, cb) {
        this._db.get('SELECT rowid, * FROM files WHERE key=$key', { $key: key }, function (err, row) {
            row = row || {};
            if (!err && _.isEmpty(row)) {
                err = new Error('404: File not found.');
            }
            cb(err, row.fileId, row);
        });
    },
    onReady: function (cb) {
        if (!cb)
            return;
        if (this._ready) {
            cb.call(this, null, this);
        } else {
            this._readyFuncs.push(cb.bind(this));
        }
    },
    listFiles: function(cb) {
        var self = this;
        this._db.all('SELECT rowId, * FROM files', function (err, rows) {
            if (err)
                return cb(err);
            async.each(rows, function (row, cb) {
                self.getStats(row.key, function (err, stats) {
                    if (!err) {
                        row.md5 = stats.custom.md5;
                        row.stamp = stats.stamp;
                        row.length = stats.length;
                    }
                    cb();
                });
            }, function () {
                cb(err, rows);
            });
        });
    },
    insertFile: function (key, fileStream, finishedCb) {
        var md5 = null,
            rowId = null,
            fileId = null,
            self = this;

        var generateHash = function (cb) {
            var md5sum = crypto.createHash('md5');
            fileStream.on('data', function (d) {
                md5sum.update(d);
            });

            fileStream.on('end', function () {
                md5 = md5sum.digest('hex');
                cb();
            });
        };

        var saveFile = function (cb) {
            self._storage.insert(key, fileStream, function (err, identifier, stat) {
                fileId = identifier;
                rowId = identifier;
                cb(err);
            });
        };

        var updateStorage = function (cb) {
            self._storage.update(fileId, function (err, header) {
                if (err)
                    return cb(err);
                var timeout = setTimeout(cb, 0);
                try {
                    header.custom = _.extend(header.custom || {}, {md5: md5});
                    return header;
                } catch (e) {
                    clearTimeout(timeout);
                    cb(e);
                }
            });
        };

        var updateFileId = function (cb) {
            updateFile(self, key, fileId, cb);
        };

        fileStream = typeof fileStream === 'string' ? fs.createReadStream(fileStream) : fileStream;
        async.parallel([saveFile, generateHash], function (err) {
            if (err) {
                return onError(function () {
                    finishedCb && finishedCb(err);
                });
            }

            async.parallel([updateStorage, updateFileId], function (err) {
                finishedCb && finishedCb(err, { rowId: rowId, key: key, fileId: fileId, md5: md5 });
            });
        });


        // rollback anything that possibly was committed.
        function onError (cb) {
            var funcs = [];

            if (fileId) {
                funcs.push(function (cb) {
                    self._storage.remove(fileId, cb);
                });
            }

            if (rowId) {
                funcs.push(function (cb) {
                    self._db.run('DELETE FROM files WHERE rowID=$rowId', { $rowId: rowId }, cb);
                });
            }

            async.parallel(funcs, cb);
        }
    },
    getFile: function (key, cb) {
        var self = this;
        self._getFileIdByKey(key, function (err, fileId) {
            if (err)
                return cb(err);
            self._storage.read(fileId, cb);
        });
    },
    pipeFile: function (key, stream) {
        var self = this;
        self._getFileIdByKey(key, function (err, fileId) {
            if (err)
                return cb(err);
            self._storage.pipe(fileId, stream);
        });
    },
    deleteFile: function (key, cb) {
        var self = this;
        self._getFileIdByKey(key, function (err, fileId) {
            if (err)
                return cb(err);
            self._storage.remove(fileId, function (err) {
                if (err)
                    return cb(err);
                self._db.run('DELETE FROM files WHERE key=$key', { $key: key }, cb);
            });
        });
    },
    getStats: function (key, cb) {
        var self = this;
        self._getFileIdByKey(key, function (err, fileId) {
            if (err)
                return cb(err);
            self._storage.stat(fileId, function (err, stat) {
                cb(err, stat);

            });
        });
    }
});

var busy = false;
function updateFile (self, key, fileId, cb) {
    if (busy) {
        setTimeout(function () {
            updateFile(self, key, fileId, cb);
        }, 100);
        return;
    }

    doSave();
    function doSave () {
        busy = true;
        self._db.serialize(function () {
            self._db.exec('BEGIN TRANSACTION;');
            self._db.run('DELETE FROM files WHERE key=$key', { $key: key });
            self._db.run('INSERT INTO files VALUES ($key, $fileId)', { $key: key, $fileId: fileId }, function (err) {
                if (err) {
                    if (err && err.toString() === 'Error: SQLITE_CONSTRAINT: UNIQUE constraint failed: files.fileId')
                        err = new Error('Fatal Error: File ID generated by file storage already in database');
                }
                rowId = this.lastID;
                cb(err);
                busy = false;
            });
            self._db.exec('COMMIT;');
        });
    }
}

module.exports = {
    init: function (name, cb) {
        var bucket = new Bucket(name);
        bucket.onReady(cb);
        return bucket;
    }
};