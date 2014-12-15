var chai = require('chai'),
    bucketManager,
    cleanup = require('../testCleanup.js'),
    expect = chai.expect;
describe('Buckets - correct input - ', function () {

    before(function (done) {
        bucketManager = require('../lib/buckets.js');
        cleanup(done);
    });

    beforeEach(function (done) {
        bucketManager.onReady(done);
    });

    it('should create bucket', function (done) {
        bucketManager.createBucket('test', function (err, bucketId) {
            expect(err).to.not.exist;
            expect(bucketId).to.be.equal(1);
            done();
        });
    });

    it('should error with bucket already exists', function (done) {
        bucketManager.createBucket('test', function (err, bucketId) {
            expect(err).to.be.instanceOf(Error);
            expect(err.toString().toLowerCase()).to.be.equal('error: bucket already exists');
            expect(bucketId).to.not.exist;
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

describe('Buckets - wrong inputs - ', function () {
    beforeEach(function (done) {
        bucketManager.onReady(done);
    });

    it('calls onReady without a function', function () {
        expect(bucketManager.onReady).to.not.throw(Error);
    });
});