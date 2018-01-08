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

/**
 * A utility for counting the number of objects in an image file.
 */
int
main(int argc, char *argv[])
try
{
  string imageFile;

  // Set line buffering for stdout so that printf's and log messages
  // interleave properly.
  setvbuf(stdout, NULL, _IOLBF, 1024);

  // Need external context to set log levels with OptionParser
  Context context(false);

  OptionsDescription clientOptions("ImageFileObjectCounter");
  
  OptionParser optionParser(clientOptions, argc, argv);

  uint64_t objectCount = 0;
  char lenBuffer[sizeof(uint32_t)];
  while(std::cin.read(lenBuffer, sizeof(lenBuffer))) {
    uint32_t keyLength = *((uint32_t*) lenBuffer); 
    char keyBuffer[keyLength];
    std::cin.read(keyBuffer, keyLength);
    
    std::cin.read(lenBuffer, sizeof(lenBuffer));
    uint32_t dataLength = *((uint32_t*) lenBuffer);
    char dataBuffer[dataLength];
    std::cin.read(dataBuffer, dataLength);

    objectCount++;
  }

  printf("Total Objects: %lu\n", objectCount);

  return 0;
} catch (Exception& e) {
    fprintf(stderr, "Exception: %s\n", e.str().c_str());
    return 1;
}

