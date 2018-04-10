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
#include "Transaction.h"

using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    int clientIndex;
    int numClients;
    string op;
    int count;

    // Set line buffering for stdout so that printf's and log messages
    // interleave properly.
    setvbuf(stdout, NULL, _IOLBF, 1024);

    // need external context to set log levels with OptionParser
    Context context(false);

    OptionsDescription clientOptions("TimeOp");
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

        ("op",
         ProgramOptions::value<string>(&op),
         "Name of the operation to time. Value can be one of: read, readnoexist, txread, txreadnoexist, txreadnoexistcache.")
        ("count",
         ProgramOptions::value<int>(&count),
         "Number of times to execute the operation.");
    
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

    if (op == "read") {    
      uint64_t tableId;
      tableId = client.createTable("test");
      Buffer value;

      char randomValue[100]; 
      client.write(tableId, "null", 4, randomValue, 100);

      for (int i = 0; i < count; i++) {
        uint64_t start = Cycles::rdtsc();
        try {
            client.read(tableId, "null", 4, &value);
        } catch (RAMCloud::ClientException& e) {
        } catch (RAMCloud::Exception& e) {
        } 
        uint64_t end = Cycles::rdtsc();
        LOG(NOTICE, "Op took %d microseconds.", Cycles::toMicroseconds(end-start));
      }

      client.dropTable("test");
    } else if (op == "txread") {
      uint64_t tableId;
      tableId = client.createTable("test");        

      char randomValue[100]; 
      client.write(tableId, "null", 4, randomValue, 100);

      for (int i = 0; i < count; i++) {
        Transaction tx(&client);
        Buffer value;
        
        uint64_t start = Cycles::rdtsc();
        try {
            tx.read(tableId, "null", 4, &value);
        } catch (RAMCloud::ClientException& e) {
        } catch (RAMCloud::Exception& e) {
        } 
        uint64_t end = Cycles::rdtsc();
        LOG(NOTICE, "Op took %d microseconds.", Cycles::toMicroseconds(end-start));
      }

      client.dropTable("test");

    } else if (op == "readnoexist") {    
      uint64_t tableId;
      tableId = client.createTable("test");
      Buffer value;

      for (int i = 0; i < count; i++) {
        uint64_t start = Cycles::rdtsc();
        try {
            client.read(tableId, "null", 4, &value);
        } catch (RAMCloud::ClientException& e) {
        } catch (RAMCloud::Exception& e) {
        } 
        uint64_t end = Cycles::rdtsc();
        LOG(NOTICE, "Op took %d microseconds.", Cycles::toMicroseconds(end-start));
      }

      client.dropTable("test");
    } else if (op == "txreadnoexist") {
      uint64_t tableId;
      tableId = client.createTable("test");        

      for (int i = 0; i < count; i++) {
        Transaction tx(&client);
        Buffer value;
        
        uint64_t start = Cycles::rdtsc();
        try {
            tx.read(tableId, "null", 4, &value);
        } catch (RAMCloud::ClientException& e) {
        } catch (RAMCloud::Exception& e) {
        } 
        uint64_t end = Cycles::rdtsc();
        LOG(NOTICE, "Op took %d microseconds.", Cycles::toMicroseconds(end-start));
      }

      client.dropTable("test");

    } else if (op == "txreadnoexistcache") {
      uint64_t tableId;
      tableId = client.createTable("test");        
      Transaction tx(&client);

      for (int i = 0; i < count; i++) {
        Buffer value;
        
        uint64_t start = Cycles::rdtsc();
        try {
            tx.read(tableId, "null", 4, &value);
        } catch (RAMCloud::ClientException& e) {
        } catch (RAMCloud::Exception& e) {
        } 
        uint64_t end = Cycles::rdtsc();
        LOG(NOTICE, "Op took %d microseconds.", Cycles::toMicroseconds(end-start));
      }

      client.dropTable("test");

    }
 


    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
