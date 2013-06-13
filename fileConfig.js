/**
 * fileDirectory = root directory to dump statsd data to
 * retention = time (in ms) to keep files dumped in file system
 */
{
  port: 8125
, backends: [ "./backends/file" ]
, flushInterval: 10000
, fileDirectory: "/var/log/statsd/"
, retention: 120000
}
