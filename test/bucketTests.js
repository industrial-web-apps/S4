var chai = require('chai'),
    bucketManager = require('../lib/buckets.js'),
    expect = chai.expect;

describe('Buckets', function () {
    before(function (done) {
        bucketManager.onReady(done);
    });

    it('should create bucket', function (done) {
        bucketManager.createBucket('test', function (err, bucketId) {
            expect(err).to.not.exist;
            expect(bucketId).to.be.equal(1);
            done();
        });
    });

    it('should error: bucket already exists', function (done) {
        bucketManager.createBucket('test', function (err, bucketId) {
            expect(err.toString().toLowerCase()).to.be.equal('error: bucket already exists');
            expect(bucketId).to.not.exist;
            expect(err).to.be.instanceOf(Error);
            done();
        });
    });

    it('should delete bucket', function (done) {
        bucketManager.deleteBucket('test', function (err) {
            expect(err).to.not.exist;
            bucketManager.listBuckets(function (err, buckets) {
                expect(buckets.length).to.be.equal(0);
            });
            done();
        });
    });
});
