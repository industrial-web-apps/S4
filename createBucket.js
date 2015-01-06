var bucketManager = require('./lib/buckets.js');
var args = process.argv.slice(2);
var bucket = args[0];

if (!bucket) {
    console.error('Bucket required');
    process.exit(1);
}
bucketManager.createBucket(bucket, function (err, bucketId) {
    if (err) {
        console.error(err);
    } else {
        console.log('bucketId: ', bucketId);
    }
    process.exit(err ? 1 : 0);
});
