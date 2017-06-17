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
#include "Transaction.h"
#include "TimeTrace.h"

using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    int clientIndex;
    int numClients;
    string tableName;
    int serverSpan;
    int objectSize;
    int asyncReadSize;
    int count;

    // Set line buffering for stdout so that printf's and log messages
    // interleave properly.
    setvbuf(stdout, NULL, _IOLBF, 1024);

    // need external context to set log levels with OptionParser
    Context context(false);

    OptionsDescription clientOptions("TimeTransactionsAsyncReads");
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

        ("serverSpan",
         ProgramOptions::value<int>(&serverSpan),
         "Server span for the table.")
        ("objectSize",
         ProgramOptions::value<int>(&objectSize),
         "Size of objects in bytes.")
        ("asyncReadSize",
         ProgramOptions::value<int>(&asyncReadSize),
         "Number of objects to asynchronously read.")
        ("count",
         ProgramOptions::value<int>(&count),
         "Number of times to execute the set of asynchronous reads.");
    
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

    uint64_t tableId;
    client.dropTable("test");
    tableId = client.createTable("test", serverSpan);
    
    // Write in values to read
    int keys[asyncReadSize];
    for (int i = 0; i < asyncReadSize; i++) {
      keys[i] = i;
      char randomValue[objectSize];
      client.write(tableId, (char*)&keys[i], sizeof(int), randomValue, 
          objectSize);
    }

    printf("%12s %12s %12s %12s %12s %12s %12s %12s %12s %12s\n", 
        "Size(B)",
        "Objects",
        "Count",
        "Min",
        "Max",
        "Avg",
        "50th",
        "90th",
        "95th",
        "99th");

    // Time asynchronous reads
    uint64_t startTime, endTime;
    uint64_t latency[count];
    for (int i = 0; i < count; i++) {
      Transaction tx(&client);
      Tub<Transaction::ReadOp> requests[asyncReadSize];
      Buffer values[asyncReadSize];

      startTime = Cycles::rdtsc();

      for (int i = 0; i < asyncReadSize; i++) {
        requests[i].construct(&tx, tableId, (char*)&keys[i], sizeof(int), 
            &values[i], true);
      }

      for (int i = 0; i < asyncReadSize; i++) {
        requests[i].get()->wait();
      }

      endTime = Cycles::rdtsc();
      latency[i] = endTime - startTime;

      tx.commit();
    }
    std::vector<uint64_t> latencyVec (latency, latency+count);

    std::sort(latencyVec.begin(), latencyVec.end());

    uint64_t sum = 0;
    for (int i = 0; i < count; i++) {
      sum += latencyVec[i];
    }

    printf("%12d %12d %12d %12.3f %12.3f %12.3f %12.3f %12.3f %12.3f %12.3f\n", 
        objectSize, 
        asyncReadSize,
        count,
        Cycles::toNanoseconds(latencyVec[0])/1000.0,
        Cycles::toNanoseconds(latencyVec[count-1])/1000.0,
        Cycles::toNanoseconds(sum)/((float)count)/1000.0,
        Cycles::toNanoseconds(latencyVec[count*50/100])/1000.0,
        Cycles::toNanoseconds(latencyVec[count*90/100])/1000.0,
        Cycles::toNanoseconds(latencyVec[count*95/100])/1000.0,
        Cycles::toNanoseconds(latencyVec[count*99/100])/1000.0);

    client.dropTable("test");

    TimeTrace::printToLog();

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
