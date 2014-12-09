var sqlite3 = require('sqlite3').verbose(),
    async = require('async'),
    bucketsDB = new sqlite3.Database('dbs/buckets.db'),
    bucket = require('./bucket.js'),
    ready = false;

var readyFuncs = [];

bucketsDB.serialize(function () {
    var commands = [
        'CREATE TABLE buckets (name TEXT)',
        'CREATE UNIQUE INDEX IF NOT EXISTS name_idx on buckets (name);'
    ];
    async.each(commands, bucketsDB.run.bind(bucketsDB), setReady);
});

module.exports = {
    onReady: function (cb) {
        if (!cb)
            return;
        if (ready) {
            cb.apply(this);
        } else {
            readyFuncs.push(cb.bind(this));
        }
    },
    getBucket: function (name, cb) {
        bucket.init(name, cb);
    },
    createBucket: function (name, cb) {
        bucketsDB.run('INSERT INTO buckets VALUES ($name)', { $name: name }, function (err) {
            if (err && err.toString() === 'Error: SQLITE_CONSTRAINT: UNIQUE constraint failed: buckets.name')
                err = new Error('Bucket already exists');
            cb && cb(err, this.lastID);
        });
    },
    listBuckets: function (cb) {
        bucketsDB.all('SELECT rowid AS id, name FROM BUCKETS', cb);
    },
    deleteBucket: function (name, cb) {
        bucketsDB.run(
            'DELETE FROM buckets WHERE name=$name',
            { $name: name }, cb
        );
    }
};

function setReady() {
    ready = true;
    readyFuncs.forEach(function (cb) { cb && cb(); });
    readyFuncs = null;
}
