#!/usr/bin/env node

const aws = require('aws-sdk')
let treehash = require('treehash')
const fs = require('fs')
const path = require('path')
let glacier = new aws.Glacier({apiVersion: '2012-06-01',region:'ap-southeast-1'})

let myarg = process.argv.slice(2)
let vaultName = myarg[0]
let filePath = myarg[1]
let partSize = 512 * 1024 * 1024

console.log("Calculating hash. Takes some time.....")
let fileName = path.basename(filePath)
let {size:filesize} = fs.statSync(filePath)
let fd = fs.openSync(filePath,'r')
let thStream = treehash.createTreeHashStream ();
let fileStream = fs.createReadStream(filePath);
fileStream.on('data', function(chunk) {
    thStream.update(chunk);
});

fileStream.on('end', function (){
    let sha = thStream.digest()
    disk(sha)
});

async function disk(sha){
    try {
        let numParts = Math.ceil(filesize / partSize)

        console.log('Initiating upload to', vaultName);
        let init = glacier.initiateMultipartUpload({
            vaultName: vaultName,
            partSize: partSize.toString(),
            archiveDescription: fileName
        }).promise()
        let multipart = await init
        console.log("Got upload ID", multipart.uploadId)

        let current = 0
        for (let i = 0; i < numParts; i++) {
            let buffer = Buffer.alloc(partSize);

            let read = fs.readSync(fd, buffer, 0, partSize, current)

            let body = buffer.slice(0, read)

            console.log(`Uploading part ${i+1}/${numParts}`)
            let upload = glacier.uploadMultipartPart({
                vaultName: vaultName,
                uploadId: multipart.uploadId,
                range: `bytes ${current}-${current + read - 1}/*`,
                body
            }).promise()

            let mdata = await upload

            current += partSize
        }

        console.log("Completing upload...");
        let final = glacier.completeMultipartUpload({
            vaultName: vaultName,
            uploadId: multipart.uploadId,
            archiveSize: filesize.toString(),
            checksum: sha
        }).promise()

        let data = await final
        console.log('Archive ID:', data.archiveId);
    }catch (e) {
        console.error(e)
    }
}
