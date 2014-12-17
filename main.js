var express = require('express');
var app = express();
var bucketManager = require('./lib/buckets.js');
var xml = require('xml');
var busboy = require('connect-busboy');

app.use(busboy());

/*********************
 * Object Operations *
 * *******************
 */

/**
 * deleteObject
 * ------------
 * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#deleteObject-property
 */
app.delete('/*/*', function (req, res) {
    var info = parseUrl(req.url);
    res.set('Content-Type', 'text/xml');
    if (!info.bucket) {
        return res.status(403).send(formulateError({
            code: 'Access Denied',
            message: 'Bucket name required'
        })).end();
    }
    bucketManager.onReady(function () {
        bucketManager.getBucket(info.bucket, function (err, bucket) {
            if (err)
                return res.status(403).send(formulateError({
                    code: 'Access Denied',
                    message: err.toString()
                })).end();
            bucket.deleteFile(info.key, function (err) {
                if (err)
                    return res.status(403).send(formulateError({
                        code: 'Access Denied',
                        message: err.toString()
                    })).end();

                res.status(200).end();
            });
        });
    });
});

/**
 * headObject
 * ----------
 * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#headObject-property
 */
app.head('/*/*', function (req, res) {
    var info = parseUrl(req.url);
    res.set('Content-Type', 'text/xml');
    if (!info.bucket) {
        return res.status(403).send(formulateError({
            code: 'Access Denied',
            message: 'Bucket name required'
        })).end();
    }
    bucketManager.onReady(function () {
        bucketManager.getBucket(info.bucket, function (err, bucket) {
            if (err)
                return res.status(403).send(formulateError({
                    code: 'Access Denied',
                    message: err.toString()
                })).end();

            bucket.getMD5Hash(info.key, function (err, md5) {
                if (err)
                    return res.status(403).send(formulateError({
                        code: 'Access Denied',
                        message: err.toString()
                    })).end();
                res.setHeader('ETag', '"' + md5 + '"');
                res.status(200).end();
            });
        });
    });
});

/**
 *  getObject
 *  ---------
 *  http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#getObject-property
 */
app.get('/*/*', function (req, res) {
    var info = parseUrl(req.url);
    res.set('Content-Type', 'text/xml');
    if (!info.bucket) {
        return res.status(403).send(formulateError({
            code: 'Access Denied',
            message: 'Bucket name required'
        })).end();
    }
    bucketManager.onReady(function () {
        bucketManager.getBucket(info.bucket, function (err, bucket) {
            if (err)
                return res.status(403).send(formulateError({
                    code: 'Access Denied',
                    message: err.toString()
                })).end();
            bucket.getFile(decodeURIComponent(info.key), function (err, stream, stat) {
                if (err) {
                    var code = 'Error';
                    if (err.toString() === 'Error: 404: File not found.') {
                        code = 'NoSuchKey';
                        err = 'The specified key does not exist.';
                    }
                    return res.status(404).send(formulateError({
                        code: code,
                        message: err.toString()
                    })).end();
                }

                res.setHeader('content-type', stat.type);
                res.status(200);
                stream.pipe(res);
            });
        });
    });
});

app.use(function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

/**
 * postObject ( for HTML forms )
 * -----------------------------
 * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html
 */
app.post('/*', function (req, res) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    var info = parseUrl(req.url);
    var form = {};
    req.busboy.on('field', function(fieldname, val) {
        form[fieldname] = val;
    });

    var called = false;
    req.busboy.on('file', function(fieldname, file) {
        // only allow single file upload
        if (called)
            return;
        called = true;
        bucketManager.onReady(function () {
            bucketManager.getBucket(info.bucket, function (err, bucket) {
                if (err)
                    return res.status(403).send(formulateError({
                        code: 'Access Denied',
                        message: err.toString()
                    })).end();

                bucket.insertFile(form.key, file, function (err, file) {
                    if (err)
                        return res.status(403).send(formulateError({
                            code: 'Access Denied',
                            message: err.toString()
                        })).end();
                    var fullUrl = req.protocol + '://' + req.get('host') + req.originalUrl;
                    res.writeHead(204, {
                        etag: '"' + file.md5 + '"',
                        location: fullUrl + '/' + form.key
                    });
                    res.end();
                });
            });
        });
    });

    req.pipe(req.busboy);
});


/**
 * putObject
 * ---------
 * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#putObject-property
 */
app.put('/*/*', function (req, res) {
    var info = parseUrl(req.url);
    res.set('Content-Type', 'text/xml');
    if (!info.bucket) {
        return res.status(403).send(formulateError({
            code: 'Access Denied',
            message: 'Bucket name required'
        })).end();
    }
    bucketManager.onReady(function () {

        bucketManager.getBucket(info.bucket, function (err, bucket) {
            if (err)
                return res.status(403).send(formulateError({
                    code: 'Access Denied',
                    message: err.toString()
                })).end();

            bucket.insertFile(info.key, req, function (err, file) {
                if (err)
                    return res.status(403).send(formulateError({
                        code: 'Access Denied',
                        message: err.toString()
                    })).end();
                res.header('ETag', '"' + file.md5 + '"');
                res.status(200).end();
            });
        });
    });
});

/*********************
 * Bucket Operations *
 * *******************
 */

/**
 * listBuckets
 * -----------
 * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#listBuckets-property
 */
app.get('/', function (req, res) {
    var info = parseUrl(req.url);
    bucketManager.onReady(function () {
        bucketManager.listBuckets(function (err, buckets) {
            var x = '<?xml version="1.0" encoding="UTF-8"?>\n' +
            xml({
                ListAllMyBucketsResult: [
                    { _attr: { xmlns: "http://s3.amazonaws.com/doc/2006-03-01/" } },
                    { Buckets: buckets.map(function (b) {
                        return {
                            Bucket: [{ Name: b.name}]
                        };
                    }) }
                ]
            });
            res.status(200).send(x).end();

        });
    });
});

/**
 * deleteBucket
 * ------------
 * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#deleteBucket-property
 */
app.delete('/*', function (req, res) {
    var info = parseUrl(req.url);
    res.set('Content-Type', 'text/xml');
    if (!info.bucket) {
        return res.status(403).send(formulateError({
            code: 'Access Denied',
            message: 'Bucket name required'
        })).end();
    }
    bucketManager.onReady(function () {
        bucketManager.deleteBucket(info.bucket, function (err) {
            if (err)
                return res.status(500).send(formulateError({
                    code: 'Error',
                    message: err.toString()
                })).end();
            res.status(200).end();
        });
    });
});

/**
 * createBucket
 * ------------
 * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#createBucket-property
 */
app.put('/*', function (req, res) {
    var info = parseUrl(req.url);
    res.set('Content-Type', 'text/xml');
    if (!info.bucket) {
        return res.status(403).send(formulateError({
            code: 'Access Denied',
            message: 'Bucket name required'
        })).end();
        return;
    }

    bucketManager.onReady(function () {
        bucketManager.createBucket(info.bucket, function (err/*, bucketId */) {
            if (err)
                return res.status(403).send(formulateError({
                    code: 'Access Denied',
                    message: err.toString()
                })).end();
            res.header('Location', '/' + info.bucket);
            res.status(200).end();
        });
    });
});

// Webs server setup
var port = process.env.PORT || 7000;
app.listen(port);

module.exports = app;

function formulateError(opts) {
    var html = "";
    return '<?xml version="1.0" encoding="UTF-8"?>\n' +
        xml({
            Error: [
                { Code: opts.code },
                { Message: opts.message },
                { RequestId: 'not implemented' },
                { hostId: 'not implemented' }
            ]
        });
}

function parseUrl (url) {
    var domainRemoved = url
        .replace('s3.amazonaws.com', '')
        .replace(':443', '')
        .replace('http://', '')
        .replace('https://', '')
        .replace(/\?.*$/, '')
        .replace(/(^\/|\/$)/g, ''); // beginning or ending '/' chars

    var pieces = domainRemoved.split('/');
    return {
        bucket: decodeURIComponent(pieces[0]),
        key: decodeURIComponent(pieces[1])
    };
}