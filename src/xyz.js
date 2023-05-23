
import checkAndReturnValidActionToken from "./utils/checkAndReturnValidAccessToken.js";
import logger from "./utils/logger.js";
import https from "https";
import axios from "axios";
import { pahubrestservicesAPI, decodeDataAPI } from "./utils/apiCalls.js";
import { configs } from "./config/config.js";
import { client, otherclient } from "./utils/databaseConnection.js";
import { config } from "dotenv";
import e from "express";

const instance = axios.create({
  httpsAgent: new https.Agent({
    // rejectUnauthorized: false,
  }),
});

export async function notificationTrigger(
  skipCount,
  limitCount,
  timer,
  retryCount,
  response
) {
  let bouncedData = {};
  let bouncedData_last = {};
  let recipientArr = [];
  let promisesArray = [];
  let decodedFirstNameArr = [];
  let dataArr = [];
  let edpcommsfmcauditObj = {};
  let nthRecordCount = {};
  let condition_and_cUpdTsEqtoTs
  const headers = {
    'APIToken': configs.pahubrestservicesAPIToken
  };
  try {
    await client.connect();
    var database = client.db(configs.databaseName);
    var db = otherclient.db(configs.databaseName);

    var rawDataCollectionName = db.collection(configs.rawDataCollectionName);
    var accessDefinitionKeyInfo = database.collection(
      configs.accessDefinitionKeyInfo
    );
    console.log(configs.accessDefinitionKeyInfo);
    var testDataCollectionName = db.collection(configs.testDataCollectionName);
    var accessDefinitionKeyInfo_arr = await accessDefinitionKeyInfo
      .find({})
      .toArray();

    var edpcommsfmcaudit = db.collection(configs.edpcommsfmcauditName);
    edpcommsfmcauditObj = await edpcommsfmcaudit
      //.find({ processName: "callBackPahub" }, { toTs: 1 }).sort({ toTs: 1 }).toArray();
      /*************order by top 1 with desc *************** */
      //.find({processName : "callBackPahub" },{"TOP 1 *": "*"}).sort({"toTs" : -1});
      .find({ processName: "callBackPahub" }, { "TOP 1 *": "*" }).sort({ "toTs": -1 }).limit(1).toArray();
    /*************************** */
    //.find({processName : "callBackPahub" },{toTs: 1}).sort({toTs: 1});

    console.log("edpcommsfmcaudit length", edpcommsfmcauditObj.length);
    //console.log("edpcommsfmcaudit toTs", edpcommsfmcauditObj[0].toTs);
    console.log(typeof edpcommsfmcauditObj);
    //Object.keys(edpcommsfmcauditObj).length

    // nthRecordCount = await testDataCollectionName
    //   .find({ notificationEvent: { "$nin": ["NotTriggered"] }, origSrcSystemCode: "PAHUB", c_upd_Ts: { $gt: edpcommsfmcauditObj[0].toTs } })
    //   .sort({ c_upd_Ts: 1 }).skip(skipCount).limit(5).toArray();
    // console.log("nthRecordCount count", nthRecordCount.length);

    // condition_and_cUpdTsEqtoTs = await testDataCollectionName
    //   .find({ notificationEvent: { "$nin": ["NotTriggered"] }, origSrcSystemCode: "PAHUB", c_upd_Ts: edpcommsfmcauditObj[0].toTs })
    //   .sort({ c_upd_Ts: 1 }).skip(skipCount).toArray();
    // console.log("condition_and_cUpdTsEqtoTs", condition_and_cUpdTsEqtoTs.length);

    // if (nthRecordCount.length = condition_and_cUpdTsEqtoTs.length) {
    //   console.log("go ahead and fetch source records with the {Condition}  AND c_upd_Ts > toTs");
    // } else if (nthRecordCount.length != condition_and_cUpdTsEqtoTs.length) {
    //   console.log("we need to include records from source with the {Condition} AND c_upd_Ts = toTs AND c_nthRecCallBackPahubFlag not equal to Y");
    // }
    bouncedData_last = await testDataCollectionName
      .find({ UUID: /^AGADIA/, notificationEvent: { "$nin": ["NotTriggered"] }, origSrcSystemCode: "PAHUB" }, { "notificationEvent": 1, "origSrcSystemCode": 1, "c_upd_Ts": 1 })
      .sort({ c_upd_Ts: 1 })
      //.skip(skipCount)
      //.limit(5)
      .toArray();
    var count_last = bouncedData_last.length;
    console.log("count_last", count_last);
    if (edpcommsfmcauditObj.length == 0) {

      bouncedData = await testDataCollectionName
        .find({ UUID: /^AGADIA/, notificationEvent: { "$nin": ["NotTriggered"] }, origSrcSystemCode: "PAHUB" }, { "notificationEvent": 1, "origSrcSystemCode": 1, "c_upd_Ts": 1 })
        .sort({ c_upd_Ts: 1 })
        //.skip(skipCount)
        .limit(5)
        .toArray();
      var count = bouncedData.length;
      console.log("bouncedData: " + bouncedData);
      /**************************************************** */
      if (bouncedData.length) {
        try {
          bouncedData.forEach((data, index) => {

            db.collection(configs.testDataCollectionName).updateOne(
              { _id: data._id },
              {
                $set: {
                  c_nthRecCallBackPahubFlag: "Y"

                },
              }
            );

            var StatusDescription = "";
            if (data.notificationEvent === "EmailSent") {
              StatusDescription = data.status;
            }
            else {
              StatusDescription = data.reason;
            };
            var StatusDateTime = "";
            if (data.notificationEvent === "EmailSent") {
              StatusDateTime = data.emailSentDate;
            }
            else if (data.notificationEvent === "EmailNotSent") {
              StatusDateTime = data.emailNotSentDate;
            }
            else if (data.notificationEvent === "EmailBounced") {
              StatusDateTime = data.emailBouncedDate;
            }
            else {

            }

            dataArr.push({
              status: data.notificationEvent,
              StatusDescription: StatusDescription,
              StatusDateTime: StatusDateTime,
              UUID: data.UUID,
              //c_nthRecCallBackPahubFlag: "Y"
            });

          });
        } catch (err) {
          logger.error(`${err.message}, Invalid Dat entered`);
          throw new Error("Unable to fetch data", { cause: err });
        }
        try {

          for (let i = 0; i < dataArr.length; i++) {
            recipientArr.push({
              status: dataArr[i].status,
              StatusDescription: dataArr[i].StatusDescription,
              StatusDateTime: (dataArr[i].StatusDateTime).toISOString(),
              UUID: dataArr[i].UUID,
              //c_nthRecCallBackPahubFlag:dataArr[i].c_nthRecCallBackPahubFlag
            });
            console.log("recipientArr ----- >", recipientArr);
          }
        } catch (err) {
          console.log(err.message);
          logger.error(`${err.message}, Unable to create recipient data Array`);
          throw new Error(" Error calling pahubrestservice API", { cause: err });
        }
        try {
          logger.info("Calling pahubrestservice API");
          var result = [];
          await instance.post(`${configs.pahubrestservicesAPI}`, recipientArr, { headers })
            .then(async (response) => {
              //receive response
              if (response.status == 200) {
                const sfmcAudit = {
                  fromTs: bouncedData[0]["c_upd_Ts"],
                  toTs: bouncedData[bouncedData.length - 1]["c_upd_Ts"],
                  //toTs: bouncedData[bouncedData.length - 1]["c_upd_Ts"],
                  numOfRecords: count,
                  nthRecordCount: bouncedData.length,
                  ProcessedTime: new Date(),
                  processName: "callBackPahub",
                  sfIntgRequestId: "",
                  serviceName: "PAHUB Call Back Services",
                  kafkaTopic: ""
                };
                // await db.collection("edpcommsfmcaudit").updateOne(
                //   { _id: value._id },
                //   {
                //     $set:
                //       sfmcAudit
                //   });
                await db.collection("edpcommsfmcaudit").insertOne(sfmcAudit);
              } else {
                console.log("AGADIA UUIDs not found");
              }
              console.log("response >>>>>>>", response);
              result.push({ response });
              //response.status(200).json({ status: 'success' });
            })

          console.log("results", result);
        } catch (err) {
          if (err?.response?.data?.errorcode && err?.response?.data?.errorcode > 0 && err.response.data.responses !== undefined) {
            console.log(err);
          } else {
            // var timer = setInterval(() => notificationTrigger(count,recalculateCount,timer,retrycount), 1000*60*0.5);
            logger.error(`${err.message}, Unable to trigger pahubrestservice API`);
            throw new Error("Unable to trigger pahubrestservice API", {
              cause: err,
            });
          }
        }

      }
      /***************************************************** */
    }
    else {
      nthRecordCount = await testDataCollectionName
        .find({ notificationEvent: { "$nin": ["NotTriggered"] }, origSrcSystemCode: "PAHUB", c_upd_Ts: { $gt: edpcommsfmcauditObj[0].toTs } })
        .sort({ c_upd_Ts: 1 }).skip(skipCount).limit(5).toArray();
      console.log("nthRecordCount count", nthRecordCount.length);

      condition_and_cUpdTsEqtoTs = await testDataCollectionName
        .find({ notificationEvent: { "$nin": ["NotTriggered"] }, origSrcSystemCode: "PAHUB", c_upd_Ts: edpcommsfmcauditObj[0].toTs })
        .sort({ c_upd_Ts: 1 }).skip(skipCount).toArray();
      console.log("condition_and_cUpdTsEqtoTs", condition_and_cUpdTsEqtoTs.length);

      if (nthRecordCount.length === condition_and_cUpdTsEqtoTs.length) {
        console.log("go ahead and fetch source records with the {Condition}  AND c_upd_Ts > toTs");
        console.log("edpcommsfmcauditObj toTs", edpcommsfmcauditObj[0].toTs);
        bouncedData = await testDataCollectionName
          .find({ UUID: /^AGADIA/, notificationEvent: { "$nin": ["NotTriggered"] }, origSrcSystemCode: "PAHUB", c_upd_Ts: { $gt: edpcommsfmcauditObj[0].toTs } })
          .sort({ c_upd_Ts: 1 })
          .skip(skipCount)
          .limit(5)
          .toArray();
        var count = bouncedData.length;
        console.log("bouncedData: " + bouncedData);
        /**************************************************** */
        if (bouncedData.length) {
          try {
            bouncedData.forEach((data, index) => {

              var StatusDescription = "";
              if (data.notificationEvent === "EmailSent") {
                StatusDescription = data.status;
              }
              else {
                StatusDescription = data.reason;
              };
              var StatusDateTime = "";
              if (data.notificationEvent === "EmailSent") {
                StatusDateTime = data.emailSentDate;
              }
              else if (data.notificationEvent === "EmailNotSent") {
                StatusDateTime = data.emailNotSentDate;
              }
              else if (data.notificationEvent === "EmailBounced") {
                StatusDateTime = data.emailBouncedDate;
              }
              else {

              }

              dataArr.push({
                status: data.notificationEvent,
                StatusDescription: StatusDescription,
                StatusDateTime: StatusDateTime,
                UUID: data.UUID
              });
            });
          } catch (err) {
            logger.error(`${err.message}, Invalid Dat entered`);
            throw new Error("Unable to fetch data", { cause: err });
          }
          try {

            for (let i = 0; i < dataArr.length; i++) {
              recipientArr.push({
                status: dataArr[i].status,
                StatusDescription: dataArr[i].StatusDescription,
                StatusDateTime: (dataArr[i].StatusDateTime).toISOString(),
                UUID: dataArr[i].UUID
              });
              console.log("recipientArr ----- >", recipientArr);
            }
          } catch (err) {
            console.log(err.message);
            logger.error(`${err.message}, Unable to create recipient data Array`);
            throw new Error(" Error calling pahubrestservice API", { cause: err });
          }
          try {
            logger.info("Calling pahubrestservice API");
            var result = [];
            await instance.post(`${configs.pahubrestservicesAPI}`, recipientArr, { headers })
              .then(async (response) => {
                //receive response
                //result.push({ response });
                //result.forEach(async function (value) {
                if (response.status == 200) {
                  const sfmcAudit = {
                    fromTs: bouncedData[0]["c_upd_Ts"],
                    toTs: bouncedData[bouncedData.length - 1]["c_upd_Ts"],
                    //toTs: edpcommsfmcauditObj[0].toTs,
                    //toTs: bouncedData[bouncedData.length - 1]["c_upd_Ts"],
                    numOfRecords: count,
                    ProcessedTime: new Date(),
                    processName: "callBackPahub",
                    sfIntgRequestId: "",
                    serviceName: "PAHUB Call Back Services",
                    kafkaTopic: ""
                  };
                  // await db.collection("edpcommsfmcaudit").updateOne(
                  //   { _id: value._id },
                  //   {
                  //     $set:
                  //       sfmcAudit
                  //   });
                  await db.collection("edpcommsfmcaudit").insertOne(sfmcAudit);
                } else {
                  console.log("AGADIA UUIDs not found");
                }
                console.log("response >>>>>>>", response);

              }); //response.status(200).json({ status: 'success' });
            // });

            console.log("results", result);
          } catch (err) {
            if (err?.response?.data?.errorcode && err?.response?.data?.errorcode > 0 && err.response.data.responses !== undefined) {
              console.log(err);
            } else {
              // var timer = setInterval(() => notificationTrigger(count,recalculateCount,timer,retrycount), 1000*60*0.5);
              logger.error(`${err.message}, Unable to trigger pahubrestservice API`);
              throw new Error("Unable to trigger pahubrestservice API", {
                cause: err,
              });
            }
          }

        }
        /***************************************************** */
      } else if (nthRecordCount.length != condition_and_cUpdTsEqtoTs.length) {
        console.log("we need to include records from source with the {Condition} AND c_upd_Ts = toTs AND c_nthRecCallBackPahubFlag not equal to Y");

        console.log("edpcommsfmcauditObj toTs", edpcommsfmcauditObj[0].toTs);
        bouncedData = await testDataCollectionName
          .find({ notificationEvent: { "$nin": ["NotTriggered"] }, c_nthRecCallBackPahubFlag: { "$nin": ["Y"] }, origSrcSystemCode: "PAHUB", c_upd_Ts: { $gte: edpcommsfmcauditObj[0].toTs } })
          //.sort({ c_upd_Ts: 1 })
          .skip(skipCount)
          //.limit(1)
          .toArray();
        var count = bouncedData.length;
        console.log("bouncedData: " + bouncedData);
        /**************************************************** */
        if (bouncedData.length) {
          try {
            bouncedData.forEach((data, index) => {

              db.collection(configs.testDataCollectionName).updateOne(
                { _id: data._id },
                {
                  $set: {
                    c_nthRecCallBackPahubFlag: "Y"
  
                  },
                }
              );

              var StatusDescription = "";
              if (data.notificationEvent === "EmailSent") {
                StatusDescription = data.status;
              }
              else {
                StatusDescription = data.reason;
              };
              var StatusDateTime = "";
              if (data.notificationEvent === "EmailSent") {
                StatusDateTime = data.emailSentDate;
              }
              else if (data.notificationEvent === "EmailNotSent") {
                StatusDateTime = data.emailNotSentDate;
              }
              else if (data.notificationEvent === "EmailBounced") {
                StatusDateTime = data.emailBouncedDate;
              }
              else {

              }

              dataArr.push({
                status: data.notificationEvent,
                StatusDescription: StatusDescription,
                StatusDateTime: StatusDateTime,
                UUID: data.UUID
              });
            });
          } catch (err) {
            logger.error(`${err.message}, Invalid Dat entered`);
            throw new Error("Unable to fetch data", { cause: err });
          }
          try {

            for (let i = 0; i < dataArr.length; i++) {
              recipientArr.push({
                status: dataArr[i].status,
                StatusDescription: dataArr[i].StatusDescription,
                StatusDateTime: (dataArr[i].StatusDateTime).toISOString(),
                UUID: dataArr[i].UUID
              });
              console.log("recipientArr ----- >", recipientArr);
            }
          } catch (err) {
            console.log(err.message);
            logger.error(`${err.message}, Unable to create recipient data Array`);
            throw new Error(" Error calling pahubrestservice API", { cause: err });
          }
          try {
            logger.info("Calling pahubrestservice API");
            var result = [];
            await instance.post(`${configs.pahubrestservicesAPI}`, recipientArr, { headers })
              .then(async (response) => {
                //receive response
                //result.push({ response });
                //result.forEach(async function (value) {
                if (response.status == 200) {
                  const sfmcAudit = {
                    fromTs: bouncedData[0]["c_upd_Ts"],
                    toTs: bouncedData[bouncedData.length - 1]["c_upd_Ts"],
                    //toTs: edpcommsfmcauditObj[0].toTs,
                    //toTs: bouncedData[bouncedData.length - 1]["c_upd_Ts"],
                    numOfRecords: count,
                    ProcessedTime: new Date(),
                    processName: "callBackPahub",
                    sfIntgRequestId: "",
                    serviceName: "PAHUB Call Back Services",
                    kafkaTopic: ""
                  };
                  // await db.collection("edpcommsfmcaudit").updateOne(
                  //   { _id: value._id },
                  //   {
                  //     $set:
                  //       sfmcAudit
                  //   });
                  await db.collection("edpcommsfmcaudit").insertOne(sfmcAudit);
                } else {
                  console.log("AGADIA UUIDs not found");
                }
                console.log("response >>>>>>>", response);

              }); //response.status(200).json({ status: 'success' });
            // });

            console.log("results", result);
          } catch (err) {
            if (err?.response?.data?.errorcode && err?.response?.data?.errorcode > 0 && err.response.data.responses !== undefined) {
              console.log(err);
            } else {
              // var timer = setInterval(() => notificationTrigger(count,recalculateCount,timer,retrycount), 1000*60*0.5);
              logger.error(`${err.message}, Unable to trigger pahubrestservice API`);
              throw new Error("Unable to trigger pahubrestservice API", {
                cause: err,
              });
            }
          }
        }
        /***************************************************** */
      }
    }
  } catch (err) {
    console.log(err.message);
    // logger.error(`${err.message}, Unable to trigger messaging API`);
    logger.error(`${err.message}, Unable to fetch data`);
    throw new Error("Unable to fetch data", { cause: err });
  }

}
==================================================================================================================================
  	
Bounce Back Notification - Call Back Services	
	
PAHUB	
The scheduler job needs to be running every 15 mins to invoke the call back service based on the below conditions:	
	
Condition - notificationEvent NOT EQUAL to "NotTriggered" AND origSrcSystemCode = "PAHUB" 	
Record Limit - 150 Records	
	
1.Read the audit collection to find an entry with processName = "callBackPahub" and latest toTs	
	
2.If we have entry in audit collection execute the below steps, ELSE skip to step 3	
	
2a. Get nthRecordCount count from the last batch and query the source table with the {Condition} AND c_upd_Ts = toTs (nth timestamp from the audit collection) to get the record count	
         2b. If both record counts matches we can go ahead and fetch source records with the {Condition}  AND c_upd_Ts > toTs	
         2c. If both record counts doesn’t match, we need to include records from source with the {Condition} AND c_upd_Ts = toTs (nth timestamp from the audit collection) AND c_nthRecCallBackPahubFlag not equal to Y. 	
	
AND Also include records with the {Condition} AND c_upd_Ts > toTs and skip to step 4	
	
Example - 2023-04-11T18:51:25.111Z The milli second highlighted is consider as the nth timestamp. 	
	
3. If we don’t have any records in audit collect (first time run) just fetch records from Ecomm collection having {Condition}	
4. Sort the records based on c_upd_Ts - Ascending order	
5. For the nth timestamp where we are ending the particular batch to a limit (example 150 records) , get all the records for the nth timestamp and add it to the existing batch even if it is more than the limit (example 150).. For the nth timestamp records update the c_nthRecCallBackPahubFlag as Y in the doc DB and insert the audit collection field nthRecordCount with the number of records with the nth timestamp.	
6. Fetch all the records and invoke the call back service based on the below request mapping	
API url - https://pilot.pahub.com/pahubrestservices/api/AcceptCOMM_Response	
"Header

APIToken
oVM/o5arX9U2mEZtydT+MsowteGQenWlbnJ6TY7tqfU="	
Fields	Mapping
Status	notificationEvent
StatusDescription	"if notificationEvent = ""EmailSent"" map from  status
ELSE 
populate from reason"
StatusDateTime	"if notificationEvent = ""EmailSent"" map emailSentDate
if notificationEvent = ""EmailNotSent"" map emailNotSentDate
if notificationEvent = ""EmailBounced"" map emailBouncedDate"
UUID	UUID
	
7. Update the Audit collection as below:	
	
fromTs --> first records c_upd_Ts	
toTs --> last fetched records c_upd_Ts (nth timestamp)	
numOfRecords --> update the number of records process in the run	
nthRecordCount --> number of records with the nth timestamp	
processedTime --> Current date ts 	
processName --> callBackPahub	
serviceName	
	
	
	
	
[9:34 AM] John, Fremid	
	
Everyone Hi I made a correction to the nth timestamp logic 	
	
	
[9:35 AM] John, Fremid	
	
AND Also include records with the {Condition} AND c_upd_Ts > toTs and skip to step 4 - updated the texted marked in bold	
	
	
[9:35 AM] John, Fremid	
	
Uploaded to confluence 	
