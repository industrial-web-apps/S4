var fse = require('fs-extra'),
    chai = require('chai'),
    MockRes = require('mock-res'),
    bucketManager,
    expect = chai.expect;

describe('Files - correct input - ', function () {
    var bucket;
    before(function (done) {
        setup();
        bucketManager.onReady(function () {
            bucketManager.createBucket('test', function () {
                bucketManager.getBucket('test', function (err, b) {
                    bucket = b;
                    done();
                });
            });
        });
    });

    it('insert into bucket', function (done) {
        var stream = fse.createReadStream('testFile.txt');
        bucket.insertFile('1234/abcd/testFile.txt', stream, function (err, fileId) {
            expect(err).to.not.exist;
            expect(fileId).to.be.equal(1);
            done();
        });
    });


    it('reads the file that was just created', function (done) {
        var content = fse.readFileSync('testFile.txt', 'utf8');
        bucket.getFile(1, function (err, stream) {
            expect(err).to.not.exist;
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

    it('deletes the file', function (done) {
        bucket.deleteFile(1, function (err) {
            expect(err).to.not.exist;
            done();
        });
    });

    it('verifies file is gone', function (done) {
        bucket.getFile(1, function (err, stream) {
            expect(err).to.be.instanceOf(Error);
            expect(err.toString().toLowerCase()).to.be.equal('error: 404: file not found.');
            expect(stream).to.not.exist;
            done();
        });
    });
});

function setup() {
    try {
        var dirsToEmpty = ['dbs', 'files'];
        dirsToEmpty.forEach(function (dir) {
            fse.removeSync(dir);
            fse.mkdirSync(dir);
        });
    } catch (e) {console.log(e);}
    bucketManager = require('../lib/buckets.js');
}