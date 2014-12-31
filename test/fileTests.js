var fse = require('fs-extra'),
    chai = require('chai'),
    MockRes = require('mock-res'),
    cleanup = require('../testCleanup.js'),
    bucketManager,
    expect = chai.expect;

describe('Files - correct input - ', function () {
    var bucket;
    var key = '1234/abcd/testFile.txt';
    before(function (done) {
        cleanup(function () {
            bucketManager = require('../lib/buckets.js');
            bucketManager.onReady(function () {
                bucketManager.createBucket('test', function () {
                    bucketManager.getBucket('test', function (err, b) {
                        bucket = b;
                        done();
                    });
                });
            });
        });
    });

    it('insert into bucket', function (done) {
        var stream = fse.createReadStream('testFile.txt');
        bucket.insertFile(key, stream, function (err, file) {
            expect(err).to.not.exist;
            expect(file.fileId).to.be.equal(1);
            expect(file.rowId).to.be.equal(1);
            expect(file.md5).to.be.equal('954c779488b31fdbe52e364fa0a71045');
            expect(file.key).to.be.equal(key);
            done();
        });
    });


    it('reads the file that was just created', function (done) {
        var content = fse.readFileSync('testFile.txt', 'utf8');
        bucket.getFile(key, function (err, stream, stat) {
            expect(err).to.not.exist;
            expect(stat.custom.md5).to.be.equal('954c779488b31fdbe52e364fa0a71045');
            var str = '';
            stream.on('data', function (x) {
                str += x;
            });
            stream.on('end', function () {
                expect(str).to.be.equal(content);
                done();
            });
        });
    });

    it('checks the md5 hash', function (done) {
        bucket.getStats(key, function (err, stat) {
            expect(stat && stat.custom && stat.custom.md5).to.be.equal('954c779488b31fdbe52e364fa0a71045');
            done();
        });
    });

    it('pipes the file to a stream', function (done) {
        var content = fse.readFileSync('testFile.txt', 'utf8');
        var stream = new MockRes();
        bucket.pipeFile(key, stream);
        var str = '';
        stream.on('data', function (x) {
            str += x;
        });
        stream.on('end', function () {
            expect(str).to.be.equal(content);
            done();
        });
    });

    it('deletes the file', function (done) {
        bucket.deleteFile(key, function (err) {
            expect(err).to.not.exist;
            done();
        });
    });

    it('verifies file is gone', function (done) {
        bucket.getFile(key, function (err, stream) {
            expect(err).to.be.instanceOf(Error);
            expect(err.toString().toLowerCase()).to.be.equal('error: 404: file not found.');
            expect(stream).to.not.exist;
            done();
        });
    });
});