'use strict';

const AWS = require('aws-sdk');
const s3 = new AWS.S3({ region: 'us-east-2' });
const sqs = new AWS.SQS({ region: 'us-east-2' });

const csv = require('csv');
const fs = require('fs');
// var transform = require('stream-transform');

// const transformer = csv.transform(
//   (record) => { 
//     const [storeId, itemId, name, category, price, amount] = record;
//     console.log('csv record2: ', JSON.stringify({ storeId, itemId, name, category, price, amount }));
//     const item = JSON.stringify({ storeId, itemId, name, category, price, amount });

//     const params = {
//       MessageBody: item,
//       QueueUrl: 'https://sqs.us-east-2.amazonaws.com/647294180507/queue_shoe_store_inventory_replenishment',
//       DelaySeconds: 0
//     }

//     sqs.sendMessage(params, (err, data) => {
//       if (err) {
//         console.log(err);
//       } else {
//         console.log(`item published: ${item}`);
//       }
//     });

//     return item;
//   }
// );

module.exports.loadTxn = (event, context, callback) => {  
    const bucketName = process.env.bucketName;
    const keyName = event.Records[0].s3.object.key;
    const params = {Bucket: bucketName, Key: keyName};

    // fs.createReadStream('data.csv')
    s3.getObject(params).createReadStream()
      .pipe(csv.parse())
      .pipe(csv.transform(
        (record) => { 
          const [storeId, itemId, name, category, price, amount] = record;
          console.log('csv record2: ', JSON.stringify({ storeId, itemId, name, category, price, amount }));
          const item = JSON.stringify({ storeId, itemId, name, category, price, amount });
      
          const params = {
            MessageBody: item,
            QueueUrl: 'https://sqs.us-east-2.amazonaws.com/647294180507/queue_shoe_store_inventory_replenishment',
            DelaySeconds: 0
          }; 
      
          sqs.sendMessage(params, (err, data) => {
            if (err) {
              console.log(err);
            } else {
              console.log(`item published: ${item}`);
            }
          });
      
          return item;
        }
      ));
};         
