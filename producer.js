var fs = require('fs');
var csvparse = require('csv-parse');
var async = require('async');

var csvFile = 'recipes.csv';
var kafka = require('kafka-node'),
    Producer = kafka.HighLevelProducer,
    KeyedMessage = kafka.KeyedMessage,
    client  = new kafka.Client(),
    producer = new Producer(client);

producer.createTopics(['recipe_A-H','recipe_I-P','recipe_Q-Z'],true,function(err,data){});

producer.on('ready',function(){
  console.log("producer is ready");
  fs.createReadStream(csvFile).pipe(parse);
});

producer.on('error',function(err){
  console.log(err);
});


var parse = csvparse({delimiter:','},function(err,data){
  async.eachSeries(data,function(line,callback) {
      processMessage(line).then(function(){
        callback();
      });
  });
});

var processMessage = function(line){
  return new Promise(function(resolve,reject){
    var recipe_name = line[1];
    var num_rep = recipe_name.charCodeAt(0)-97;
    var topic_name;
    if (num_rep < 8){
      topic_name = 'recipe_A-H';
    }else if (num_rep>=8 && num_rep<16){
      topic_name = 'recipe_I-P';
    }
    else{
      topic_name = 'recipe_Q-Z';
    }
    var recipe_doc = {
      'id':line[0],
      'name':line[1],
      'description':line[2],
      'ingredient':line[3],
      'active':line[4],
      'updated_date':line[5],
      'created_date':line[6]
    };

    recipeMsg = new KeyedMessage(recipe_doc.id,JSON.stringify(recipe_doc));
    payloads = [{
      topic: topic_name, messages: recipeMsg, partition:3,
    }];
    producer.send(payloads,function(err,data){
      if(!err){
        resolve();
      }else{
        reject();
      }
    });
  });



}
