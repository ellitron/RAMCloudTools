/* Copyright (c) 2009-2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>

#include <iostream>
#include <fstream>

#include "ClusterMetrics.h"
#include "Context.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "ShortMacros.h"
#include "Crc32C.h"
#include "ObjectFinder.h"
#include "OptionParser.h"
#include "RamCloud.h"
#include "Tub.h"
#include "IndexLookup.h"
#include "TableEnumerator.h"

using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    int clientIndex;
    int numClients;
    string tableName;
    int serverSpan;
    string imageFileName;

    // Set line buffering for stdout so that printf's and log messages
    // interleave properly.
    setvbuf(stdout, NULL, _IOLBF, 1024);

    // need external context to set log levels with OptionParser
    Context context(false);

    OptionsDescription clientOptions("TableUploader");
    clientOptions.add_options()

        // These first two options are currently ignored. They're here so that
        // this script can be run with cluster.py.
        ("clientIndex",
         ProgramOptions::value<int>(&clientIndex)->
            default_value(0),
         "Index of this client (first client is 0; currently ignored)")
        ("numClients",
         ProgramOptions::value<int>(&numClients)->
            default_value(1),
         "Total number of clients running (currently ignored)")

        ("tableName",
         ProgramOptions::value<string>(&tableName),
         "Name of the table to image.")
        ("serverSpan",
         ProgramOptions::value<int>(&serverSpan),
         "Server span for the table.")
        ("imageFile",
         ProgramOptions::value<string>(&imageFileName),
         "Name of the image file to create.");
    
    OptionParser optionParser(clientOptions, argc, argv);
    context.transportManager->setSessionTimeout(
            optionParser.options.getSessionTimeout());

    LOG(NOTICE, "Connecting to %s",
        optionParser.options.getCoordinatorLocator().c_str());

    string locator = optionParser.options.getExternalStorageLocator();
    if (locator.size() == 0) {
        locator = optionParser.options.getCoordinatorLocator();
    }
    RamCloud client(&context, locator.c_str(),
            optionParser.options.getClusterName().c_str());

    LOG(NOTICE, "Uploading table %s with server span %d from file %s", tableName.c_str(), serverSpan, imageFileName.c_str());

    uint64_t tableId;
    tableId = client.createTable(tableName.c_str(), serverSpan);

    std::ifstream inFile;
    inFile.open(imageFileName.c_str(), std::ios::binary);

    int objCount = 0;
    int byteCount = 0;
    char lenBuffer[sizeof(uint32_t)];
    while(inFile.read(lenBuffer, sizeof(lenBuffer))) {
        uint32_t keyLength = *((uint32_t*) lenBuffer); 
        char keyBuffer[keyLength];
        inFile.read(keyBuffer, keyLength);
        string key(keyBuffer, keyLength);

        inFile.read(lenBuffer, sizeof(lenBuffer));
        uint32_t dataLength = *((uint32_t*) lenBuffer);
        char dataBuffer[dataLength];
        inFile.read(dataBuffer, dataLength);
        string data(dataBuffer, dataLength);

//        LOG(NOTICE, "[%s, %s]", key.c_str(), data.c_str());

        objCount++;
        byteCount += keyLength + dataLength;
        if (objCount % 100000 == 0) {
          LOG(NOTICE, "Uploaded %d objects totalling %d bytes.", objCount, byteCount);
        }
    }

    inFile.close();

    LOG(NOTICE, "Table uploaded.");

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
