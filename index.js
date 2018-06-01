var request = require("request");
var fs = require('fs');
var AWS = require('aws-sdk');
var path = require('path');

var s3 = new AWS.S3();
AWS.config.update({
    accessKeyId: "AKIAJJU6AFC2JJWJGRUA",
    secretAccessKey: "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"
});

var myBucket = 'taxisgdata';

var options = {
    url: 'https://api.data.gov.sg/v1/transport/taxi-availability',
    headers: {
        'api-key': 'pgFB7sqxSFOdQdQboZs1nd25utoLlyJy'
    }
};

var lineCount = 0;
var currDate = new Date();
var padString = function(str) {
    var _str = String(str);
    if (_str.length == 1) {
        return '0' + _str;
    } else 
        return _str;
};
var getCurrDateFormatted = function() {
    return padString(currDate.getDate()) + '_' + padString(currDate.getMonth()) + '_' + currDate.getFullYear();
};
var outfileName = 'taxi_' + getCurrDateFormatted() +'.txt';

var startFileOutStream = function() {
    var logger = fs.createWriteStream(outfileName, {
        flags: 'a' // 'a' means appending (old data will be preserved)
    });
}

startFileOutStream();

var callback = function(error, response, body) {
    if (!error && response.statusCode == 200) {
        var taxiData = JSON.parse(body);

        var lineData = '';

        if (taxiData.features.length > 0) {
            var feature = taxiData.features[0];
            feature.geometry.coordinates.forEach(function(location) {
                lineData = feature.properties.timestamp + ',' + location[0] + ',' + location[1] + "\r\n";
                lineCount++;
                logger.write(lineData);
            });
        }

    }
};

var timeInterval = 1000 * 60;
setInterval(function() {
    var datetimeNow = new Date();
    if (datetimeNow.getDate() != currDate.getDate()) {
        // some console messages
        console.log(lineCount + ' lines written');

        // close previous stream
        logger.end();

        // send previous file to s3
        filePath = outfileName;
        bucketParams = {
            Bucket: myBucket,
            Body : fs.createReadStream(filePath),
            Key : path.basename(filePath)
        };
        s3.upload(bucketParams, function (err, data) {
            //handle error
            if (err) {
              console.log("Error", err);
            }
            //success
            if (data) {
              console.log("Uploaded in:", data.Location);
            }
        });

        // open new stream to new file
        outfileName = 'taxi_' + getCurrDateFormatted() + '.txt';
        startFileOutStream();
    }
    request.get(options, callback);
}, timeInterval);