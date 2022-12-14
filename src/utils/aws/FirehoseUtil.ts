import AWS from 'aws-sdk';
import _ from 'underscore';


export default class FirehoseStreamer {
  private _firehose:AWS.Firehose;
  
  constructor(conf?:AWS.Firehose.ClientConfiguration) {
    this._firehose = new AWS.Firehose(conf);
  }

  public postRecord(record:any, firehoseStream:string) : Promise<AWS.Firehose.PutRecordOutput> {
    return new Promise((resolve, reject) => {
      try {
        const params:AWS.Firehose.PutRecordInput = {
          DeliveryStreamName : firehoseStream,
          Record : { Data : JSON.stringify(record) }
        };
        
        this._firehose.putRecord(params, function(err, data) {
          if (err) {
            console.log(`Track ${firehoseStream}: Failed on AWS`);
            console.log(err, err.stack);
            reject(err);
          } else {
            resolve(data);
          }
        });
      } catch(e) {
        console.log(`Track ${firehoseStream}: Failed`);
        console.log("Error", e.stack);
        console.log("Error", e.name);
        console.log("Error", e.message);
        reject(e);
      }
    })
  }

  public postData(rows:any[], firehoseStream:string) : Promise<AWS.Firehose.PutRecordBatchOutput> {
    return new Promise((resolve, reject) => {
      try {
        const params:AWS.Firehose.PutRecordBatchInput = {
          DeliveryStreamName: firehoseStream,
          Records : _.map(rows, r => { 
            const record:AWS.Firehose.Record = {
              Data : JSON.stringify(r)
            };
            return record;
          })
        };
        this._firehose.putRecordBatch(params, function(err, data) {
          if (err) {
            console.log(`Track ${firehoseStream}: Failed on AWS`);
            console.log(err, err.stack);
            reject(err);
          } else {
            resolve(data);
          }
        });
      } catch(e) {
        console.log(`Track ${firehoseStream}: Failed`);
        console.log("Error", e.stack);
        console.log("Error", e.name);
        console.log("Error", e.message);
        reject(e);
      }
    });
  }
}