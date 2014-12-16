var fse = require('fs-extra'),
    async = require('async'),
    sqlite3 = require('sqlite3').verbose();

module.exports = function (done, cleanup) {
    try {
        fse.removeSync('files');
        fse.mkdirSync('files');
        async.each(fse.readdirSync('dbs'), function (db, cb) {
            if (/\.db$/.test(db)) {
                db = new sqlite3.Database('dbs/' + db);
                db.all('select name from sqlite_master where type = "table";', function (err, tbls) {
                    db.exec(tbls.map(function (row) {
                        return 'DELETE FROM ' + row.name + ' WHERE 1=1;';
                    }).join(' '), function () {
                        cb();
                    });
                });
            }
        }, done);
    } catch (e) {
        console.log(e);
    }
};
