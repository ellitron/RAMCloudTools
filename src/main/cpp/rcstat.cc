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
#include "PerfStats.h"

using namespace RAMCloud;

/**
 * Given the raw response returned by CoordinatorClient::serverControlAll,
 * divide it up into individual PerfStats objects for each server, and
 * store those in an array indexed by server id.
 *
 * \param rawData
 *      Response buffer from a call to CoordinatorClient::serverControlAll.
 * \param[out] results
 *      Filled in (possibly sparsely) with contents parsed from rawData.
 *      Entry i will contain PerfStats for the server whose ServerId has
 *      indexNumber i. Empty entries have 0 collectionTimes.
 */
void
parseStats(Buffer* rawData, std::vector<PerfStats>* results)
{
    results->clear();
    uint32_t offset = sizeof(WireFormat::ServerControlAll::Response);
    while (offset < rawData->size()) {
        WireFormat::ServerControl::Response* header =
                rawData->getOffset<WireFormat::ServerControl::Response>(offset);
        offset += sizeof32(*header);
        if ((header == NULL) ||
                ((offset + sizeof32(PerfStats)) > rawData->size())) {
            break;
        }
        uint32_t i = ServerId(header->serverId).indexNumber();
        if (i >= results->size()) {
            results->resize(i+1);
        }
        rawData->copy(offset, header->outputLength, &results->at(i));
        offset += header->outputLength;
    }
}
  
int
main(int argc, char *argv[])
try
{
    int clientIndex;
    int numClients;
    string serverLocator;
    int interval;

    // Set line buffering for stdout so that printf's and log messages
    // interleave properly.
    setvbuf(stdout, NULL, _IOLBF, 1024);

    // need external context to set log levels with OptionParser
    Context context(false);

    OptionsDescription clientOptions("PerfStats");
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

        ("serverLocator",
         ProgramOptions::value<string>(&serverLocator),
         "Location of the server to get stats for.")

        ("interval",
         ProgramOptions::value<int>(&interval)->
            default_value(1),
         "Reporting interval for statistics in seconds (default: 1s).");
     
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

    bool printHeader=true;
    std::vector<PerfStats> prevStats;
    std::vector<PerfStats> currStats;
    while(true) {
      Buffer statBuf;
      client.serverControlAll(WireFormat::ControlOp::GET_PERF_STATS, NULL, 0, 
          &statBuf);

      parseStats(&statBuf, &currStats);
      if (printHeader) {
        for (size_t i = 0; i < currStats.size(); i++) {
          PerfStats& p = currStats[i];
          if (p.collectionTime == 0) {
            continue;
          }
          char columnTitle[50];
          sprintf(columnTitle, "%d.reads", i);
          printf("%12s", columnTitle);
          sprintf(columnTitle, "(+diff)");
          printf("%12s", columnTitle); 
        }
        printf("\n");
        printHeader=false;
      } 

      for (size_t i = 0; i < currStats.size(); i++) {
        PerfStats& p = currStats[i];
        if (p.collectionTime == 0) {
          continue;
        }
        printf("%12d", p.readCount);

        int diff = 0;
        if (prevStats.size() == currStats.size()) {
          diff = p.readCount - prevStats[i].readCount;
        } 

        char column[50];
        sprintf(column, "(+ %d)", diff);
        printf("%12s", column);
      }
      printf("\n");

      prevStats = currStats;

      Cycles::sleep((uint64_t)interval*1000000);
    }
    /*
    Cycles::sleep(1000000);
    Buffer afterStats;
    client.serverControlAll(WireFormat::ControlOp::GET_PERF_STATS, NULL, 0, 
        &afterStats);
    string stats = PerfStats::printClusterStats(&statBuf, &afterStats)
        .c_str(); 

    printf("%s", stats.c_str());
    */

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
