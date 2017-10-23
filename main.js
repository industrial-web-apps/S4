var bucketManager = require('./lib/buckets.js'),
    xml = require('xml'),
    URL = require('url'),
    crypto = require('crypto'),
    moment = require('moment'),
    busboy = require('connect-busboy'),
    _ = require('lodash'),
    fs = require('fs'),
    express = require('express'),
    AWS = require('aws-sdk');
    V4Signer = require('aws-sdk/lib/signers/v4');
    app = express();

var user = JSON.parse(fs.readFileSync('user.json', 'utf8'));

app.use(busboy());

process.on('uncaughtException', function (err) {
    console.log('=================');
    console.log(err.stack);
    console.log('=================');
});

// must be on top so it checks auth on all requests handlers declared after it.
app.use(function(req, res, next) {
    res.set('Content-Type', 'text/xml');
    var info = parseUrl(req.url);
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");

    if (req.method === 'OPTIONS')
        return next();
    // TODO: would be nice to have a way to parse the form
    // fields at this point to do the authentication here,
    // but not parse the files, let the handler do that.
    // for now Auth is assumed it will be done in the POST handler
    if (req.method === 'POST')
        return next();

    if (req.headers.authorization) {
        if (req.headers.authorization.startsWith('AWS key:')) {
            var signer = new Signer(req);
            var auth = signer.getAuthorization(user, new Date());
            if (req.headers.authorization === auth)
                return next();
        } else if (req.headers.authorization.startsWith('AWS4-HMAC-SHA256')) {
            var signer = new V4Signer({
                method: req.method,
                headers: {
                    host: req.headers.host,
                    'x-amz-content-sha256': req.headers['x-amz-content-sha256'],
                    'x-amz-date': req.headers['x-amz-date'],
                },
                pathname() { return req.path; },
                search() { return ''; },
                region: 'us-east-1',
            }, 's3');
            const datetime = AWS.util.date.iso8601(new Date()).replace(/[:\-]|\.\d{3}/g, '');
            signer.isPresigned = () => true;
            if (req.headers.authorization === signer.authorization(user, datetime))
                return next();
        }
    }

    var inboundURL = URL.parse(req.url, true);
    if (inboundURL.query.Signature) {
        var expiry = Number(inboundURL.query.Expires);
        if (isNaN(expiry) || !isFinite(expiry) || Date.now()/1000 > expiry) {
            return res.status(403).send(formulateError({
                code: 'Access Denied',
                message: 'Policy expired'
            })).end();
        }

        var policy = 'GET\n\n\n' + inboundURL.query.Expires + '\n';
        policy += '/' + info.bucket + '/' + encodeURIComponent(info.key);
        if (inboundURL.query['response-content-disposition']) {
            policy += '?response-content-disposition=' +
                inboundURL.query['response-content-disposition'];
        }

        var signature = crypto.createHmac("sha1", user.secretAccessKey)
            .update(policy).digest('base64');
        if (inboundURL.query.Signature === signature)
            return next();
    }

    return res.status(403).send(formulateError({
        code: 'Access Denied',
        message: 'Authorization failed'
    })).end();
});

/*********************
 * Object Operations *
 * *******************
 */

/**
 * deleteObject
 * ------------
 * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#deleteObject-property
 */
app.delete(/\/.+\/.+/, function (req, res) {
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
app.head(/\/.+\/.+/, function (req, res) {
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

            bucket.getStats(info.key, function (err, stat) {
                if (err)
                    return res.status(403).send(formulateError({
                        code: 'Access Denied',
                        message: err.toString()
                    })).end();
                var md5 = stat.custom && stat.custom.md5,
                    length = stat.length || 0;
                 if (md5)
                    res.setHeader('ETag', '"' + md5 + '"');
                if (typeof length === 'number')
                    res.setHeader('Content-Length', length);
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
app.get(/\/.+\/.+/, function (req, res) {
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
                    code: 'BucketNotFound',
                    message: err.toString()
                })).end();
            bucket.getFile(info.key, function (err, stream, stat) {
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

                var url_parts = URL.parse(req.url, true);
                var query = url_parts.query;
                if (query['response-content-disposition'])
                    res.setHeader('Content-Disposition', query['response-content-disposition']);
                res.setHeader('content-type', stat.type);
                res.setHeader('content-length', stat.length);
                res.setHeader('etag', stat.custom && stat.custom.md5);
                res.status(200);
                stream.pipe(res);
            });
        });
    });
});

/**
 * postObject ( for HTML forms )
 * -----------------------------
 * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html
 */
app.post('/*', function (req, res) {
    var info = parseUrl(req.url);
    var form = {};
    req.busboy.on('field', function(fieldname, val) {
        if (/content-length/i.test(fieldname))
            fieldname = fieldname.toLowerCase();
        form[fieldname] = val;
    });

    var called = false;
    req.busboy.on('file', function(fieldname, file) {
        // only allow single file upload
        if (called)
            return;
        called = true;

        if (!allowed(_.extend(form, { bucket: info.bucket }))) {
            file.on('data', function () {});
            file.on('end', function () {
                res.status(403).send(formulateError({
                    code: 'Access Denied',
                    message: 'Authorization failed'
                })).end();
            });
            return;
        }

        bucketManager.onReady(function () {
            bucketManager.getBucket(info.bucket, function (err, bucket) {
                if (err) {
                    file.on('data', function () {});
                    file.on('end', function () {
                        res.status(403).send(formulateError({
                            code: 'Access Denied',
                            message: err.toString()
                        })).end();
                    });
                    return;
                }

                bucket.insertFile(form.key, file, function (err, fileStat) {
                    if (err) {
                        res.status(403).send(formulateError({
                            code: 'Access Denied',
                            message: err.toString()
                        })).end();
                        return;
                    }
                    var fullUrl = req.protocol + '://' + req.get('host') + req.originalUrl;
                    res.writeHead(204, {
                        etag: '"' + fileStat.md5 + '"',
                        location: fullUrl + '/' + form.key
                    });
                    res.end();
                });
            });
        });
    });

    req.pipe(req.busboy);

    function allowed(info) {
        try {
            var policy = new Buffer(info.policy, 'base64').toString();
            policy = JSON.parse(policy);

            var x = moment(policy.expiration).toDate();
            // check if expired.
            if (x.getTime() < Date.now())
                return false;

            if (user.accessKeyId !== info.AWSAccessKeyId)
                return false;

            // ensure policy is correct
            if (policy.conditions[0].bucket !== info.bucket)
                return false;

            var expectedKey = policy.conditions[1] && policy.conditions[1][2];

            if (expectedKey !== info.key)
                return false;

            // check signature
            var verifySignature = crypto.createHmac("sha1", user.secretAccessKey)
                .update(info.policy)
                .digest('base64');
            return info.signature === verifySignature;
        } catch (e) {
             return false;
        }
    }
});


/**
 * putObject
 * ---------
 * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#putObject-property
 */
app.put(/\/.+\/.+/, function (req, res) {
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


/***************************
 *  More Object Operations *
 * *************************
 *  These need to be here otherwise things to get defined in the right order
 */

/**
 *  listObjects
 *  -----------
 *  http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#listObjects-property
 */
app.get('/*', function (req, res) {
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
                    code: 'BucketNotFound',
                    message: err.toString()
                })).end();
            bucket.listFiles(function(err, files) {
                var x = '<?xml version="1.0" encoding="UTF-8"?>\n' +
                    xml({
                        ListBucketResult: [
                            { _attr: { xmlns: "http://s3.amazonaws.com/doc/2006-03-01/" } },
                            { Name: bucket.name },
                            { Prefix: '' },
                            { Marker: '' },
                            { MaxKeys: '' },
                            { IsTruncated: false }
                        ].concat(files.map(function (b) {
                            return {
                                Contents: [
                                    { Key: b.key },
                                    { ETag: '"' + b.md5 + '"' },
                                    { LastModified: new Date(b.stamp).toISOString() },
                                    { Size: b.length },
                                    { StorageClass: 'STANDARD'}
                                ]
                            };
                        }))
                    });
                res.status(200).send(x).end();
            });
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
    var key = pieces.splice(1).join('/');
    if (pieces[0].endsWith('.'))
        pieces[0] = pieces[0].substring(0, pieces[0].length - 1);
    return {
        bucket: decodeURIComponent(pieces[0]),
        key: decodeURIComponent(key)
    };
}


function Signer (req) {
    this.request = req;
}

_.extend(Signer.prototype, {
    /**
     * When building the stringToSign, these sub resource params should be
     * part of the canonical resource string with their NON-decoded values
     */
    subResources: {
        'acl': 1,
        'cors': 1,
        'lifecycle': 1,
        'delete': 1,
        'location': 1,
        'logging': 1,
        'notification': 1,
        'partNumber': 1,
        'policy': 1,
        'requestPayment': 1,
        'restore': 1,
        'tagging': 1,
        'torrent': 1,
        'uploadId': 1,
        'uploads': 1,
        'versionId': 1,
        'versioning': 1,
        'versions': 1,
        'website': 1
    },

    // when building the stringToSign, these querystring params should be
    // part of the canonical resource string with their NON-encoded values
    responseHeaders: {
        'response-content-type': 1,
        'response-content-language': 1,
        'response-expires': 1,
        'response-cache-control': 1,
        'response-content-disposition': 1,
        'response-content-encoding': 1
    },
    getAuthorization: function getAuthorization(credentials, date) {
        var signature = this.sign(credentials.secretAccessKey, this.stringToSign());
        return 'AWS ' + credentials.accessKeyId + ':' + signature;
    },
    sign: function sign(secret, string) {
        if (typeof string === 'string') string = new Buffer(string);
        return crypto.createHmac('sha1', secret).update(string).digest('base64');
    },

    stringToSign: function stringToSign() {
        var r = this.request;

        var parts = [];
        parts.push(r.method);
        parts.push(r.headers['content-md5'] || '');
        parts.push(r.headers['content-type'] || '');

        // This is the "Date" header, but we use X-Amz-Date.
        // The S3 signing mechanism requires us to pass an empty
        // string for this Date header regardless.
        parts.push(r.headers['presigned-expires'] || '');

        var headers = this.canonicalizedAmzHeaders();
        if (headers) parts.push(headers);
        parts.push(this.canonicalizedResource());

        return parts.join('\n');

    },

    canonicalizedAmzHeaders: function canonicalizedAmzHeaders() {

        var amzHeaders = [];

        _.each(Object.keys(this.request.headers), function (name) {
            if (name.match(/^x-amz-/i))
                amzHeaders.push(name);
        });

        amzHeaders.sort(function (a, b) {
            return a.toLowerCase() < b.toLowerCase() ? -1 : 1;
        });

        var parts = [];
        arrayEach.call(this, amzHeaders, function (name) {
            parts.push(name.toLowerCase() + ':' + String(this.request.headers[name]));
        });

        return parts.join('\n');

    },

    canonicalizedResource: function canonicalizedResource() {

        var r = this.request;

        var parts = r.path.split('?');
        var path = parts[0];
        var querystring = parts[1];

        var resource = '';

        if (r.virtualHostedBucket)
            resource += '/' + r.virtualHostedBucket;

        resource += path;

        if (querystring) {

            // collect a list of sub resources and query params that need to be signed
            var resources = [];

            arrayEach.call(this, querystring.split('&'), function (param) {
                var name = param.split('=')[0];
                var value = param.split('=')[1];
                if (this.subResources[name] || this.responseHeaders[name]) {
                    var subresource = { name: name };
                    if (value !== undefined) {
                        if (this.subResources[name]) {
                            subresource.value = value;
                        } else {
                            subresource.value = decodeURIComponent(value);
                        }
                    }
                    resources.push(subresource);
                }
            });

            resources.sort(function (a, b) { return a.name < b.name ? -1 : 1; });

            if (resources.length) {

                querystring = [];
                _.each(resources, function (resource) {
                    if (resource.value === undefined)
                        querystring.push(resource.name);
                    else
                        querystring.push(resource.name + '=' + resource.value);
                });

                resource += '?' + querystring.join('&');
            }

        }

        return resource;

    }
});

function arrayEach(array, iterFunction) {
    for (var idx in array) {
        if (array.hasOwnProperty(idx)) {
            var ret = iterFunction.call(this, array[idx], parseInt(idx, 10));
        }
    }
}