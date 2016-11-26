// Note : Express isn't required as this app only subcribes to the broker and posts the data to MongoDB. It should not be accessed by the user in any way.
var mongoClient = require('mongodb').MongoClient;
var mqtt = require('mqtt');
var broker_db_config = require('./broker_db_config');

var sensorDB = false;
var connection_status;

//Connect to MongoDB
mongoClient.connect(broker_db_config.mongodb_info.url, function(err, db) {
  if (!err) {
    console.log('Connected to MongoDB !');
    sensorDB = db;
  }
});

var client = mqtt.connect("mqtt://" + broker_db_config.broker_info.url + ":" + broker_db_config.broker_info.port.toString(), broker_db_config.broker_info.options);

client.on('connect', function() {
  console.log("Connected to the broker, yay !")
  client.subscribe(broker_db_config.broker_info.topic);
  connection_status = true;
});

client.on('message', function(topic, message) {
  if (connection_status && sensorDB) {
    data = JSON.parse(message.toString());
    insertSensorData(sensorDB, data, function(result) {});
  } else {
    console.log('Connection or collection not ready yet.')
  }

});

var insertSensorData = function(db, data, cb) {
  db.collection('rasp_sensor').insertOne(data, function(err, result) {
    if (!err) {
      console.log('Insert data to db succeeded');
    } else {
      console.log('Failed to insert data.')
    }
  });
};
