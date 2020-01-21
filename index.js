const util = require('util');
var JSZip = require('jszip');
var fs = require('fs');
var TrackviaAPI = require('trackvia-api');
var FormatHelper = require('./formatHelper.js');
var config = require('./config');
var log = require('./log');
const Excel = require('exceljs');

//The ID of a record
const ID_FIELD = "id";

const RECORD_ID_FIELD = "Record ID";

const LAST_USER_ID_FIELD = "Last User(id)";
const TABLES = {
  MERGE: "MERGE",
  TEMPLATE: "TEMPLATE"
};

//The TrackVia api for interaction with the data
var api = new TrackviaAPI(config.account.api_key, config.account.environment);
var formatter = new FormatHelper();



/*****************************************************
 * The section below here is where all the code goes
 * that determines the out come of this microserivce.
 * This is the fun part
 *****************************************************/

/**
 * Used by our microservice infrastructure to
 * kick off all events
 */
exports.handler = function(event, context, callback) {
    //don't let the multi-threaded nature of things
    //cause the call back to not resolve immedietly.
    if(context){
        context.callbackWaitsForEmptyEventLoop = false;
    }
    log.log('starting');
    checkTemplateViewId(config.template_table.view_id);
    checkTemplateViewId(config.merged_doc_table.view_id);
    globalCallback = callback;

    //Check if we're doing this for a single record
    //or for lots of records
    if (!event.tableId) {
            log.error('No table ID. I am out');
            globalCallback(null, "There's no table ID, so I'm done");
    } else {
        //go get the records we need to merge
        login(event.tableId);
    }
}


/**
 * Log the user in
 * @param {Number} tableId
 */
function login(tableId){
    //first figure out if we have a viewId associated
    //with this table Id
    var viewId = getViewForTable(tableId);
    log.log("ViewId is: " + viewId);
    //now login

    //check really hard for a valid access token
    if(config.account.access_token
        && typeof config.account.access_token === 'string'
        && config.account.access_token.length > 20){
            log.log("Access token seems valid, using that to authorize");
        //access token
        api.setAccessToken(config.account.access_token);
        getRecordsThatNeedToBeMerged(tableId, viewId);
    } else {
        log.log("Access token does not seem valid, using username and password");
        api.login(config.account.username, config.account.password)
        .then(()=>{
            getRecordsThatNeedToBeMerged(tableId, viewId);
        })
        .catch(function(err) {
            handleError(err);
        });
    }
}

/**
 * This function gets a tableID, finds the viewID, if one exists
 * and then grabs all the records in that view to be merged
 * @param {Number} tableId
 */
async function getRecordsThatNeedToBeMerged(tableId, viewId){
    log.log('Logged In.');
    api.getView(viewId, {"start": 0, "max": 1000})
    .then(async (response) =>{

        var data = response.data;
        var structure = response.structure;
        // search the structure of the field for an image field
        let imageFields = [];
        for(let field of structure) {
            if(field.type === 'image') {
                imageFields.push(field.name);
            }
        }
        // get all the images for the records
        let imagesToRetrieve = [];
        for(let record of data) {
            for(let field of imageFields) {
                // load the images in a series because API returns 404 if field is empty
                //imagesToRetrieve.push(api.getFile(viewId, record['Record ID'], field))
                try {
                    let image = await api.getFile(viewId, record['Record ID'], field);
                    imagesToRetrieve.push(image);
                } catch(err) {
                    imagesToRetrieve.push('');
                }
            }
        }
        Promise.all(imagesToRetrieve).then(imageData => {
            // update the records to contain the image contents
            for(let record of data) {
                let imagesForRecord = imageData.splice(0, imageFields.length);
                for(let index in imageFields) {
                    // fetch must have a response to contain an image
                    if(imagesForRecord[index].response) {
                        imageType = imagesForRecord[index].response.headers['content-disposition'].match(/"(.*)"/).pop().split('.')[1];
                        imageContents = imagesForRecord[index]['body'];
                        imageB64 = `data:image/${imageType};base64,${Buffer.from(imageContents, 'binary').toString('base64')}`;
                        record[imageFields[index]] = imageB64;
                    } else {
                        record[imageFields[index]] = '';
                    }
                }
            }
            log.log("Records found in view: " + data.length);
            resetSourceRecordLTP(viewId, data);

            //now figure out what templates are at play for which records
            getTemplates(viewId, data, structure);
        }).catch(function(err) {
            console.log(err)
        });
    })
    .catch(function(err) {
       handleError(err);
    });
}

/**
 * Reset the LTP for the template in the source folders
 * @param {Number} viewId
 * @param {Array} records
 */
function resetSourceRecordLTP(viewId, records){
    var resetPromises = [];
    var templateRelationshipFieldName = config.source_tables.template_relationship_field_name;
    var data = {[templateRelationshipFieldName]:null};
    records.forEach(function(record){
        resetPromises.push(api.updateRecord(viewId, record[ID_FIELD], data));
    });
    //don't care about the response
    Promise.all(resetPromises)
    .then(()=>{
        log.log("Reset all source records");
    }).
    catch(function(err){
      //check for error code, if 401 then could be, wrong relation field name,
      log.error("Unable to unset relationship to template. Please ensure template relationship field name, \"" + config.source_tables.template_relationship_field_name + "\" matches relationship name on source table EXACTLY, and be sure that \"" + viewId + "\" is the correct view for sending merge data to templates.");
    });
}


/**
 * This function will grab all the template files
 * that are needed to satisfy the current requests
 * @param {map of data} data
 */
function getTemplates(viewId, data, structure){
    //now figure out what templates are at play for which records
    var templatesToRecords = getDistinctTemplateForRecords(data);
    //for each template create a promise and go get them all
    var promises = [];
    var templateIdsInOrder = [];
    for(id in templatesToRecords){
        templateIdsInOrder.push(id);
        promises.push(api.getFile(config.template_table.view_id, id, config.template_table.field_name_for_template_document));
    }
    Promise.all(promises)
    .then((templates) => {
        var templateIdToFiles = {};
        for(i in templateIdsInOrder){
            //get the file name if it's in there
            var fileName = templates[i].response.headers["content-disposition"].replace(/"/g, '');
            if(fileName.indexOf("filename=") > 0){
                var index = fileName.indexOf("filename=") + "filename=".length;
                fileName = fileName.substr(index);
            } else {
                fileName = "template.xlsx";
            }
            log.log("Template file name is " + fileName);
            // file = templates[i].body;
            file = Buffer.from(templates[i].body, 'binary');
            templateIdToFiles[templateIdsInOrder[i]] = {"file": file, "name": fileName};
        }
        //might need to write files to disk here? Not sure.
        mergeRecordsIntoTemplates(templatesToRecords, templateIdToFiles, structure)
        .then(mergeData => {
            uploadMergeFiles(viewId, mergeData, templatesToRecords);
        });
    }).catch(function(err) {
      checkFieldNames(TABLES.TEMPLATE, config.template_table.view_id);
    });
}

/**
 * Checks to make sure that the viewId is a number
 * @param {Number} viewId
 */
function checkTemplateViewId(viewId) {
  if (isNaN(parseInt(viewId)) || viewId <= 0) {
    log.error('Please ensure template view ids are numeric and greater than 0');
  }
  return viewId;
}

/**
 * This will take in a map of template IDs to the resultant merge files
 * and upload them in the appropriate place
 * @param {object} idsToMergeFiles
 */
function uploadMergeFiles(viewId, mergeData, templatesToRecords){
    var promises = [];
    for(id in mergeData){
        var templateMergeData = mergeData[id];
        var file = templateMergeData["file"];
        var recordIdList = templateMergeData["recordIds"];
        var userId = templateMergeData["userId"];

        var recordIdsStr = recordIdList.join("\n");
        var recordCount = templatesToRecords[id].length;
        var recordData = {};

        //if defined update the details
        if(config.merged_doc_table.merged_doc_details_field_name) {
            recordData[config.merged_doc_table.merged_doc_details_field_name] = "Merged " + recordCount + " records:\n" + recordIdsStr;
        }

        //if defined set the relationship
        if(config.merged_doc_table.merged_doc_to_template_relationship_field_name) {
            recordData[config.merged_doc_table.merged_doc_to_template_relationship_field_name] = id;
        }

        //if it's all configured, add the user who made the change
        if(config.merged_doc_table.merge_user_field_name && userId) {
            recordData[config.merged_doc_table.merge_user_field_name] = userId;
        }
        promises.push(api.addRecord(config.merged_doc_table.view_id, recordData ));
    }
    Promise.all(promises)
    .then((newRecords) =>{
        var uploadPromises = [];
        var i = 0;
        for(id in mergeData){
            var templateMergeData = mergeData[id];
            var file = templateMergeData["file"];
            var recordId = newRecords[i].data[0][ID_FIELD];
            uploadPromises.push(api.attachFile(config.merged_doc_table.view_id, recordId, config.merged_doc_table.merged_document_field_name, file));
            i++;
        }
        return Promise.all(uploadPromises);
    })
    .then((uploadResponses) =>{
        log.log("done uploading everything");

        if(globalCallback){
            globalCallback(null, "Merge completed successfully");
        }
    })
    .catch(function(err) {
      checkFieldNames(TABLES.MERGE, config.merged_doc_table.view_id);
    });
}

/**
 * Function to determine what field is causing the upload error
 * @param {String} table
 * @param {Number} viewId
 */
function checkFieldNames(table, viewId) {
  api.getView(viewId)
  .then((view) => {
    let structure = createFieldsObject(view.structure);
    for (let field in config.export_fields[table]) {
      let fieldValue = config.export_fields[table][field];
      if (!fields[fieldValue]) {
        log.error(`Couldn't find the field \"${fieldValue}\" in the table \"${table}\". This value is set in config.js as the value for \"${field}\"`);
      }
    }
  })
  .catch((err) => {
    if(err.statusCode == 401){
      log.error(`Could not find ${table} view, please check the view id: "${viewId}"`);
      return;
    }
    handleError(err);
  });
}

function createFieldsObject(structure) {
  fields = {};
  structure.forEach((field) => {
    fields[field.name] = true;
  });
  return fields;
}

/**
 * Helper function that loops over the function that does the real work
 * @param {*} templatesToRecords
 * @param {*} templateIdToFiles
 */
async function mergeRecordsIntoTemplates(templatesToRecords, templateIdToFiles, structure){
    var idsToMergeFiles = {};
    for(var templateId in templateIdToFiles){
        //get the user who last updated the source record
        var userId = getLastUpdatedUser(templatesToRecords[templateId]);
        //gets a list of record IDs for the notes
        var recordIds = getRecordIdsList(templatesToRecords[templateId]);
        var mergeFile = await mergeRecordIntoTemplate(templatesToRecords[templateId], templateIdToFiles[templateId], templateId, structure);
        idsToMergeFiles[templateId] = {"file": mergeFile, "recordIds": recordIds, "userId": userId};
    }

    return idsToMergeFiles;
}

/**
 * Gets the last user to update the record
 * @param {Map} recordList
 */
function getLastUpdatedUser(recordList){
    var userId = null;
    if(recordList.length > 0){
        userId = recordList[0][LAST_USER_ID_FIELD];
    }
    return userId;
}

/**
 * Creates a list of recordId values
 * @param {Map} recordList
 */
function getRecordIdsList(recordList){
    var list = [];
    recordList.forEach(function(record){
        var recordId = record[RECORD_ID_FIELD];
        if(recordId){
            list.push(recordId);
        }
    });
    return list;
}



/**
 * This function takes in the template
 * and the data from all the records
 * and then outputs a merged .docx file
 */
async function mergeRecordIntoTemplate(records, template, templateId, structure){
    log.log("In mergeRecordIntoTemplate");
    // Create a workbook to manipulate
    let workbook = new Excel.Workbook();
    try {
        workbook = await workbook.xlsx.load(template.file);
    } catch (err) {
        console.log("Error reading workbook template:", err)
        globalCallback(null, err);
    }
    // Get the first worksheet from the template file
    let worksheet = workbook.getWorksheet(1);

    // Get the headers from the first row of data
    let row = worksheet.getRow(1).values;

    // slice empty first item
    row = row.slice(1);

    // Set the column headers for use when adding
    columns = [];
    for(let column of row) {
        columns.push({
            header: column,
            key: column
        });
    }
    worksheet.columns = columns;
    
    // Add the rows to the worksheet
    let rowCounter = 1
    for(let record of records) {
        let recordToInsert = record;
        // Look for image fields and take special action
        let column = 0;
        for(let field in recordToInsert) {
            if(typeof recordToInsert[field] === 'string' && recordToInsert[field].slice(0, 10) === 'data:image') {
                // this may be a problem if 4 letter file ending are used with image EG ".jpeg"
                // console.log("imageType:", recordToInsert[field].slice(11, 14))
                let imageId = workbook.addImage({
                    base64: recordToInsert[field],
                    extension: recordToInsert[field].slice(11, 14),
                });
                worksheet.addImage(imageId, {
                    tl: { col: column, row: rowCounter },
                    br: { col: column+1, row: rowCounter+1 },
                    // ext: { 
                    //     width: config.template_table.image_size.width, 
                    //     height: config.template_table.image_size.height 
                    // }
                });
                delete recordToInsert[field];
            }
            column++;
        }
        worksheet.addRow(recordToInsert);
        rowCounter++;
    }

    var fileName = template.name;
    var filePath = "/tmp/" + templateId;
    if (!fs.existsSync(filePath)){
        fs.mkdirSync(filePath);
    }
    var currentTimeStr = formatter.getCurrentDateTime();
    filePath = filePath + "/" + currentTimeStr + "_" + fileName;
    try {
        await workbook.xlsx.writeFile(filePath);
    } catch (err) {
        globalCallback(null, err);
    }
    log.log("Wrote file to file systems: " + filePath);
    return filePath;
}


/**
 * Given the list of data figures out which templates
 * are needed and organizes the data by template
 * for easier merging
 * @param {list of TV record data} data
 */
function getDistinctTemplateForRecords(records, template){
    var templatesToRecords = {};
    records.forEach(function(record){
        var templateId = record[config.source_tables.template_relationship_field_name_id];
        if(templateId){
            if(!(templateId in templatesToRecords)){
                templatesToRecords[templateId] = [];
            }
            templatesToRecords[templateId].push(record);
        }
    });

    return templatesToRecords;
}


/**
 * A simple helper function to look up the
 * view ID and do some error handling
 * @param {Number} tableId
 */
function getViewForTable(tableId){
    //make sure we're using a string key
    tableId = tableId.toString();

    //check if the table is in our
    //map of tables to views
    if(!(tableId in config.source_tables.table_ids_to_view_ids)){
        var errorStr = "There's no entry in our map for table: " + tableId + ". End";
        log.error(errorStr);
        return;
    }

    //get the view ID
    return config.source_tables.table_ids_to_view_ids[tableId];
}


/**
 * All error handling goes here
 * @param {Object} err
 */
function handleError(err){
    let parsedError = {
      'status'  : err.status,
      'href'    : err.href,
      'verb'    : err.verb,
      'headers' : err.headers,
      'body'    : err.body
    }
    log.error(util.inspect(parsedError, {showHidden: false, depth: null}))
    if(globalCallback != null){
        globalCallback(null, err);
    }
}

function base64DataURLToArrayBuffer(dataURL) {
    const base64Regex = /^data:image\/(png|jpg|svg|svg\+xml);base64,/;
    if (!base64Regex.test(dataURL)) {
      return false;
    }
    const stringBase64 = dataURL.replace(base64Regex, "");
    let binaryString;
    binaryString = Buffer.from(stringBase64, "base64").toString("binary");
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
      const ascii = binaryString.charCodeAt(i);
      bytes[i] = ascii;
    }
    return bytes.buffer;
  }
