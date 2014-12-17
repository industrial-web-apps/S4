var chai = require('chai'),
    fs = require('fs'),
    expect = chai.expect,
    _ = require('lodash'),
    proxy = require('proxy-agent'),
    server,
    cleanup = require('../testCleanup.js'),
    AWS = require('aws-sdk');

describe('test S3 compatibility', function () {
    var s3;
    var bucketName = 's4Testing';
    var obj = {
        Bucket: bucketName,
        Key: 'thisIsATestKey'
    };
    before(function (done) {
        cleanup(function () {
            server = require('../main.js');
            AWS.config.update({
                sslEnabled: false,
                httpOptions: { agent: proxy('http://localhost:7000') }
            });

            AWS.config.credentials = {
                "accessKeyId": "key",
                "secretAccessKey": "secret",
                "region": "us-east-1"
            };

            s3 = new AWS.S3();
            done();
        });
    });

    it('creates a bucket', function (done) {
        s3.createBucket({
            Bucket: bucketName
        }, function (err, res) {
            expect(err).to.not.exist;
            expect(res.Location).to.be.equal('/' + bucketName);
            done();
        });
    });

    it('verifies bucket was created', function (done) {
        s3.listBuckets(function (err, res) {
            expect(err).to.not.exist;
            expect(_.pluck(res.Buckets, 'Name')).to.include(bucketName);
            done();
        });
    });

    it('puts a file in a bucket', function (done) {
        s3.putObject(_.extend({
            Body: fs.createReadStream('testFile.txt')
        }, obj), function (err, res) {
            expect(err).to.not.exist;
            expect(res.ETag).to.be.equal('"954c779488b31fdbe52e364fa0a71045"');
            done();
        });
    });

    it('retrieve a file', function (done) {
        s3.getObject(obj, function (err, res) {
            expect(err).to.not.exist;
            expect(res.Body.toString()).to.be.equal('this is a test file, nothing else.');
            done();
        });
    });

    it("verifies the file's md5 hash", function (done) {
        s3.headObject(obj, function (err, res) {
            expect(err).to.not.exist;
            expect(res.ETag).to.be.equal('"954c779488b31fdbe52e364fa0a71045"');
            done();
        });
    });

    it('deletes a file in a bucket', function (done) {
        s3.deleteObject(obj, function (err, res) {
            expect(err).to.not.exist;
            expect(res).to.be.empty;
            done();
        });
    });

    it('verifies file is gone', function (done) {
        s3.getObject(obj, function (err) {
            expect(err).to.exist;
            expect(err.toString()).to.be.equal('NoSuchKey: The specified key does not exist.');
            done();
        });
    });

    it('deletes a bucket', function (done) {
        s3.deleteBucket({ Bucket: bucketName }, function (err, res) {
            expect(err).to.not.exist;
            expect(res).to.be.empty;
            done();
        });
    });

    it('verifies bucket has been deleted', function (done) {
        s3.listBuckets(function (err, res) {
            expect(err).to.not.exist;
            expect(_.pluck(res.Buckets, 'Name')).to.not.include(bucketName);
            done();
        });
    });
});