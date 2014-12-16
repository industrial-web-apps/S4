var express = require('express');
var app = express();
var bucketManager = require('./lib/buckets.js');

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
//
//app.get('/', function(req, res) {
//    res.send("Hello Cybertron!");
//    console.log(req);
//});
//
//app.get('/insecticons.json', function(req, res) {
//    res.writeHead(200, { 'Content-Type': 'application/json' });
//    res.write(JSON.stringify({insecticons : ["Shrapnel","Bombshell", "Kickback"]}));
//    res.end();
//});

var port = process.env.PORT || 7000;
app.listen(port);

module.exports = app;

var xml = require('xml');
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
        .replace(/(^\/|\/$)/g, ''); // beginning or ending '/' chars

    var pieces = domainRemoved.split('/');
    return {
        bucket: pieces[0],
        key: pieces[1]
    };
}
