/**
 * Simple statsd backend that dumps the raw flush data to disk.
 * Checks a retention time for how long files can be kept around.
 * Also runs some simple aggregation of the data for the retention period.
 */
var fs = require('fs'),
    math = require('mathjs');

function setupMath() {
    //add some missing math library functions. they assume correct incoming data!
    math.import({
        sum: function (arr) {
            var sum = 0;
            arr.forEach(function (element) {
                sum += element;
            });
            return sum;
        },
        mean: function (arr) {
            var count = arr.length,
                sum = math.sum(arr);
            return sum / count;
        },
        stdev: function (arr) {
            var count = arr.length,
                mean = math.mean(arr),
                sum = 0;
            arr.forEach(function (element) {
                sum += math.square(element - mean);
            });
            return sum / count;
        }
    });
}

//makes a folder if it doesn't already exist
//TODO: test to ensure there is a trailing slash
//TODO: better yet, just find a library for this stuff
function createFolder(destination) {
    var index, length, path = "",
        folder = destination.substring(0, destination.lastIndexOf("/")),
        parts = folder.split("/");

    //make sure directory exists all the way down
    for (index = 0, length = parts.length; index < length; index += 1) {
        path += parts[index] + "/"; //use path.sep
        if (!fs.existsSync(path)) {
            fs.mkdirSync(path);
        }
    }
}

//cleans up the data folders based on persistence time configuration
function cleanup(folder, timestamp, retention) {

    var oldest = timestamp - retention,
        list = fs.readdirSync(folder),
        current,
        filename,
        index, length;

    console.log("Running cleanup @ " + timestamp + ", oldest timestamp = " + oldest);

    for (index = 0, length = list.length; index < length; index++) {
        current = (list[index] * 1);
        filename = folder + list[index];
        if (oldest >= current) {
            console.log("Deleting " + filename);
            fs.unlinkSync(filename);
        }
    }

};

function saveFileJson(filename, obj) {
    fs.writeFileSync(filename, JSON.stringify(obj, null, 4), "utf-8");
}

//aggregates all stats from the raw folder
//this means that the aggregate "sample time" will equal the file retention time
function aggregate(sourceFolder, outputFolder) {

    console.log("Running stat aggregation");

    var inputs = fs.readdirSync(sourceFolder),
        json = [],
        index, length,
        counters, timers, gauges;

    for (index = 0, length = inputs.length; index < length; index++) {
        json.push(JSON.parse(fs.readFileSync(sourceFolder + inputs[index], "utf-8")));
    }

    //console.log(JSON.stringify(json));


    counters = processCounters(json);
    saveFileJson(outputFolder + "counters", counters);

    timers = processTimers(json);
    saveFileJson(outputFolder + "timers", timers);

    gauges = processGauges(json);
    saveFileJson(outputFolder + "gauges", gauges);

}

//grabs all the counters for the time period, and simply sums them up in one big hash
function processCounters(stats) {

    var output = {};

    //they'll be an array, one for each flush
    stats.forEach(function (stat) {

        var counters = stat.counters;

        Object.keys(counters).forEach(function (key) {

            var counter = counters[key],
                hashed = output[key];

            if (!hashed) {
                hashed = counter;
            } else {
                hashed = counter + hashed;
            }

            output[key] = hashed;

        });

    });

    return output;

}

//for timers, store the data list and some basic stats
function processTimers(stats) {

    var output = {};

    //they'll be an array, one for each flush
    stats.forEach(function (stat) {

        var timers = stat.timers;

        Object.keys(timers).forEach(function (key) {

            var list = timers[key],
                hashed = output[key],
                arr;

            if (!hashed) {
                hashed = {};
                hashed.data = list;
            } else {
                hashed.data = hashed.data.concat(list);
            }

            arr = hashed.data;

            if (arr.length > 0) {
                hashed.stats = {};
                hashed.stats.count = arr.length;
                hashed.stats.min = math.min(arr);
                hashed.stats.max = math.max(arr);
                hashed.stats.mean = math.mean(arr);
                hashed.stats.stdev = math.stdev(arr);
            }

            output[key] = hashed;

        });

    });

    return output;

}

//gauges are just a series of values, so we'll store only the data list
//TODO: if there is a previous value, we should check for sign and modify
function processGauges(stats) {

    var output = {};

    //they'll be an array, one for each flush
    stats.forEach(function (stat) {

        var gauges = stat.gauges;

        Object.keys(gauges).forEach(function (key) {

            var gauge = gauges[key],
                hashed = output[key];

            if (!hashed) {
                hashed = [];
            }

            hashed.push(gauge);
            output[key] = hashed;

        });

    });

    return output;

}

function FileBackend(startupTime, config, emitter){
    var self = this;
    this.lastFlush = startupTime;
    this.lastException = startupTime;
    this.config = config.console || {};
    this.rootFolder = config.fileDirectory;
    this.rawFolder = this.rootFolder + "raw/";
    this.aggregateFolder = this.rootFolder + "aggregate/"
    this.retention = config.retention || 30000; //default fairly low so we don't accidentally fill up a disk

    setupMath();

    //ensure output locations are in place
    createFolder(this.rootFolder);
    createFolder(this.rawFolder);
    createFolder(this.aggregateFolder);

    // attach
    emitter.on('flush', function(timestamp, metrics) { self.flush(timestamp, metrics); });
    emitter.on('status', function(callback) { self.status(callback); });
}


FileBackend.prototype.flush = function(timestamp, metrics) {

    //we'll operate on millis so we have fairly high resolution
    var millis = timestamp * 1000,
        retention = this.retention,
        outFile = this.rawFolder + "/" + millis;

    console.log('Flushing stats at', new Date(millis).toString());
    console.log("Saving raw metrics to " + outFile);

    //store the raw metrics for later processing
    saveFileJson(outFile, metrics);

    console.log(JSON.stringify(metrics, null, 4));

    cleanup(this.rawFolder, millis, retention);

    aggregate(this.rawFolder, this.aggregateFolder);

};

FileBackend.prototype.status = function(write) {
    ['lastFlush', 'lastException'].forEach(function(key) {
        write(null, 'file', key, this[key]);
    }, this);
};

exports.init = function(startupTime, config, events) {
    var instance = new FileBackend(startupTime, config, events);
    return true;
};
