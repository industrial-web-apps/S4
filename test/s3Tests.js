var chai = require('chai'),
    fs = require('fs'),
    expect = chai.expect,
    proxy = require('proxy-agent'),
    server,
    cleanup = require('../testCleanup.js'),
    AWS = require('aws-sdk');

describe('test S3 compatibility', function () {
    var s3;
    var bucketName = 's4Testing';
    before(function (done) {
        cleanup(function () {
            server = require('../main.js');
            AWS.config.update({
                sslEnabled: false,
                httpOptions: { agent: proxy('http://localhost:7000') }
            });

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

    it('puts a file in a bucket', function (done) {
        s3.putObject({
            Bucket: bucketName,
            Key: 'thisIsATestKey',
            Body: fs.createReadStream('testFile.txt')
        }, function (err, res) {
            expect(err).to.not.exist;
            expect(res.ETag).to.be.equal('"954c779488b31fdbe52e364fa0a71045"');
            done();
        });
    });
});