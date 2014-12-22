var chai = require('chai'),
    fs = require('fs'),
    expect = chai.expect,
    _ = require('lodash'),
    proxy = require('proxy-agent'),
    server,
    FormData = require('form-data'),
    cleanup = require('../testCleanup.js'),
    AWS = require('aws-sdk');

describe('test S3 compatibility', function () {
    var s3;
    var bucketName = 's4Testing';
    //var url = 'http://s3.amazonaws.com/';
    var url = 'http://localhost:7000/';
    var obj = {
        Bucket: bucketName,
        Key: 'thisIsATestKey'
    };
    var obj2 = {
        Bucket: bucketName,
        Key: 'thisIsATestKey2'
    };

    before(function (done) {
        cleanup(function () {
            server = require('../main.js');
            AWS.config.update({
                httpOptions: { agent: proxy(url) }
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

    it('tests HTML form POST object', function (done) {
        var formData = new FormData();
        var s3Policy = require('s3policy');
        var myS3Account = new s3Policy(AWS.config.credentials.accessKeyId, AWS.config.credentials.secretAccessKey);
        var policy = myS3Account.writePolicy(obj2.Key, obj2.Bucket, 60, 4096, true);

        formData.append("AWSAccessKeyId", AWS.config.credentials.accessKeyId);
        formData.append("policy", policy.s3PolicyBase64);
        formData.append("signature", policy.s3Signature);
        formData.append("key", obj2.Key);
        formData.append("Content-Type", 'text/plain');
        formData.append("acl", "private");
        //formData.append('x-amz-server-side-encryption', 'AES256');
        formData.append("file", fs.createReadStream('testFile.txt'));


        formData.submit(url + obj2.Bucket, function (err, res) {
            // NOTE: error doesn't seem to ever be true with this library
            // even when an error is returned.
            expect(err).to.not.exist;
            expect(res.statusCode).to.be.equal(204);
            expect(res.headers.location).to.be.equal(url + obj2.Bucket + '/' + obj2.Key);
            expect(res.headers.etag).to.be.equal('"954c779488b31fdbe52e364fa0a71045"');
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