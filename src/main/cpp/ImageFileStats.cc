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
 * A utility for gathering useful stats on an image file.
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

  OptionsDescription clientOptions("ImageFileStats");
  
  OptionParser optionParser(clientOptions, argc, argv);

  uint64_t totalObjectCount = 0;
  uint64_t totalKeySize = 0;
  uint64_t totalValueSize = 0;
  uint64_t totalFileSize = 0;
  uint64_t totalObjectSize = 0;
  uint64_t totalMetadataSize = 0;
  char lenBuffer[sizeof(uint32_t)];
  while(std::cin.read(lenBuffer, sizeof(lenBuffer))) {
    totalMetadataSize += sizeof(lenBuffer);
    totalFileSize += sizeof(lenBuffer);

    uint32_t keyLength = *((uint32_t*) lenBuffer); 
    char keyBuffer[keyLength];
    std::cin.read(keyBuffer, keyLength);
    totalKeySize += (uint64_t)keyLength;
    totalObjectSize += (uint64_t)keyLength;
    totalFileSize += (uint64_t)keyLength;

    std::cin.read(lenBuffer, sizeof(lenBuffer));
    totalMetadataSize += sizeof(lenBuffer);
    totalFileSize += sizeof(lenBuffer);

    uint32_t dataLength = *((uint32_t*) lenBuffer);
    char dataBuffer[dataLength];
    std::cin.read(dataBuffer, dataLength);
    totalValueSize += (uint64_t)dataLength;
    totalObjectSize += (uint64_t)dataLength;
    totalFileSize += (uint64_t)dataLength;

    totalObjectCount++;
  }

  printf("Image File Stats:\n");
  printf("  Total File Size: %lu\n", totalFileSize);
  printf("    File Metadata Bytes: %lu, (%.1f%)\n", totalMetadataSize, 100.0 * (double)totalMetadataSize / (double)totalFileSize);
  printf("    Raw Object Bytes: %lu, (%.1f%)\n", totalObjectSize, 100.0 * (double)totalObjectSize / (double)totalFileSize);
  printf("      Total Key Bytes: %lu, (%.1f%)\n", totalKeySize, 100.0 * (double)totalKeySize / (double)totalObjectSize);
  printf("      Total Value Bytes: %lu, (%.1f%)\n", totalValueSize, 100.0 * (double)totalValueSize / (double)totalObjectSize);
  printf("  Total Object Count: %lu\n", totalObjectCount);
  printf("  Average Key Size: %lu\n", totalKeySize / totalObjectCount);
  printf("  Average Value Size: %lu\n", totalValueSize / totalObjectCount);

  return 0;
} catch (Exception& e) {
    fprintf(stderr, "Exception: %s\n", e.str().c_str());
    return 1;
}

