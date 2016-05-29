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

#include <inttypes.h>

#include <iostream>
#include <fstream>
#include <sstream>

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
 
/**
 * Check if the vector contains a particular element.
 *
 * \param v
 *      vector to search.
 * \param e
 *      element to search for.
 * \return
 *      true if found, false otherwise.
 */
bool
contains(std::vector<std::string> &v, const char *e) {
  std::string elem(e);
  if (std::find(v.begin(), v.end(), elem) != v.end()) {
    return true;
  } else {
    return false;
  }
}

int
main(int argc, char *argv[])
try
{
  int interval;
  std::string format;

  // Set line buffering for stdout so that printf's and log messages
  // interleave properly.
  setvbuf(stdout, NULL, _IOLBF, 1024);

  OptionsDescription clientOptions("PerfStats");
  clientOptions.add_options()

    ("interval",
     ProgramOptions::value<int>(&interval)->
        default_value(1),
     "Reporting interval for statistics in seconds (default: 1s).") 
    ("format",
     ProgramOptions::value<std::string>(&format)->
         default_value("mup,Mup"),
     "Format options for rcstat output. Options are comma-separated.\n"
     "  Ro   - Total number of objects read.\n"
     "  ro   - Per server number of objects read.\n"
     "  Rob  - Total number of bytes in objects read.\n"
     "  rob  - Per server number of bytes in objects read.\n"
     "  Rkb  - Total number of bytes in keys read.\n"
     "  rkb  - Per server number of bytes in keys read.\n"
     "  Wo   - Total number of objects written.\n"
     "  wo   - Per server number of objects written.\n"
     "  Wob  - Total number of bytes in objects written.\n"
     "  wob  - Per server number of bytes in objects written.\n"
     "  Wkb  - Total number of bytes in keys written.\n"
     "  wkb  - Per server number of bytes in keys written.\n"
     "  Mc   - Total memory capacity in bytes.\n"
     "  mc   - Per server memory capacity in bytes.\n"
     "  Mu   - Total amount of memory used in bytes.\n"
     "  mu   - Per server amount of memory used in bytes.\n"
     "  Mup  - Total amount of memory used as a \% of capacity.\n"
     "  mup  - Per server amount of memory used as a \% of capacity.\n"
     "  Mf   - Total amount of memory free in bytes.\n"
     "  mf   - Per server amount of memory free in bytes.\n"
     "  Mfp  - Total amount of memory free as a \% of capacity.\n"
     "  mfp  - Per server amount of memory free as a \% of capacity.\n"
     "[default: mup,Mup]");

  OptionParser optionParser(clientOptions, argc, argv);

  string locator = optionParser.options.getExternalStorageLocator();
  if (locator.size() == 0) {
      locator = optionParser.options.getCoordinatorLocator();
  }

  // Parse the format option for the output columns.
  std::vector<std::string> columns; 
  std::stringstream ss(format);
  std::string col;
  while (std::getline(ss, col, ',')) {
    columns.push_back(col);
  }

  RamCloud client(locator.c_str());

  std::vector<PerfStats> currStats;
  Buffer statBuf;
  client.serverControlAll(WireFormat::ControlOp::GET_PERF_STATS, NULL, 0, 
      &statBuf);
  parseStats(&statBuf, &currStats);

  int colWidth = 12;
  const char *colFmtStr = "%12s";

  /*
   * Print column headers.
   */
  for (size_t i = 0; i < currStats.size() - 1; i++) {
    /*
     * ro   - Per server number of objects read.
     * rob  - Per server number of bytes in objects read.
     * rkb  - Per server number of bytes in keys read.
     * wo   - Per server number of objects written.
     * wob  - Per server number of bytes in objects written.
     * wkb  - Per server number of bytes in keys written.
     * mc   - Per server memory capacity in bytes.
     * mu   - Per server amount of memory used in bytes.
     * mup  - Per server amount of memory used as a \% of capacity.
     * mf   - Per server amount of memory free in bytes.
     * mfp  - Per server amount of memory free as a \% of capacity.
     */
    if (contains(columns, "ro")) { printf(colFmtStr, "ro"); }
    if (contains(columns, "rob")) { printf(colFmtStr, "rob"); }
    if (contains(columns, "rkb")) { printf(colFmtStr, "rkb"); }
    if (contains(columns, "wo")) { printf(colFmtStr, "wo"); }
    if (contains(columns, "wob")) { printf(colFmtStr, "wob"); }
    if (contains(columns, "wkb")) { printf(colFmtStr, "wkb"); }
    if (contains(columns, "mc")) { printf(colFmtStr, "mc"); }
    if (contains(columns, "mu")) { printf(colFmtStr, "mu"); }
    if (contains(columns, "mup")) { printf(colFmtStr, "mup"); }
    if (contains(columns, "mf")) { printf(colFmtStr, "mf"); }
    if (contains(columns, "mfp")) { printf(colFmtStr, "mfp"); }
  }

  /*
   * Ro   - Total number of objects read.
   * Rob  - Total number of bytes in objects read.
   * Rkb  - Total number of bytes in keys read.
   * Wo   - Total number of objects written.
   * Wob  - Total number of bytes in objects written.
   * Wkb  - Total number of bytes in keys written.
   * Mc   - Total memory capacity in bytes.
   * Mu   - Total amount of memory used in bytes.
   * Mup  - Total amount of memory used as a \% of capacity.
   * Mf   - Total amount of memory free in bytes.
   * Mfp  - Total amount of memory free as a \% of capacity.
   */
  if (contains(columns, "Ro")) { printf(colFmtStr, "Ro"); }
  if (contains(columns, "Rob")) { printf(colFmtStr, "Rob"); }
  if (contains(columns, "Rkb")) { printf(colFmtStr, "Rkb"); }
  if (contains(columns, "Wo")) { printf(colFmtStr, "Wo"); }
  if (contains(columns, "Wob")) { printf(colFmtStr, "Wob"); }
  if (contains(columns, "Wkb")) { printf(colFmtStr, "Wkb"); }
  if (contains(columns, "Mc")) { printf(colFmtStr, "Mc"); }
  if (contains(columns, "Mu")) { printf(colFmtStr, "Mu"); }
  if (contains(columns, "Mup")) { printf(colFmtStr, "Mup"); }
  if (contains(columns, "Mf")) { printf(colFmtStr, "Mf"); }
  if (contains(columns, "Mfp")) { printf(colFmtStr, "Mfp"); }

  printf("\n");

  std::vector<PerfStats> prevStats;
  while (true) {
    sleep(interval);
    
    prevStats = currStats;

    Buffer statBuf;
    client.serverControlAll(WireFormat::ControlOp::GET_PERF_STATS, NULL, 0, 
        &statBuf);
    parseStats(&statBuf, &currStats);

    uint64_t Ro = 0;
    uint64_t Rob = 0;
    uint64_t Rkb = 0;
    uint64_t Wo = 0;
    uint64_t Wob = 0;
    uint64_t Wkb = 0;
    uint64_t Mc = 0;
    uint64_t Mu = 0;
    uint64_t Mf = 0;
    for (size_t i = 0; i < currStats.size(); i++) {
      PerfStats& c = currStats[i];
      PerfStats& p = prevStats[i];

      if (c.collectionTime == 0) {
        continue;
      }
     
      uint64_t ro = c.readCount;
      uint64_t rob = c.readObjectBytes;
      uint64_t rkb = c.readKeyBytes;
      uint64_t wo = c.writeCount;
      uint64_t wob = c.writeObjectBytes;
      uint64_t wkb = c.writeKeyBytes;
      uint64_t mc = c.logMaxLiveBytes;
      uint64_t mu = c.logLiveBytes;
      uint64_t mup = (100 * mu) / mc;
      uint64_t mf = c.logAppendableBytes;
      uint64_t mfp = (100 * mf) / mc;

      if (contains(columns, "ro")) { printf("%12llu", ro); }
      if (contains(columns, "rob")) { printf("%12llu", rob); }
      if (contains(columns, "rkb")) { printf("%12llu", rkb); }
      if (contains(columns, "wo")) { printf("%12llu", wo); }
      if (contains(columns, "wob")) { printf("%12llu", wob); }
      if (contains(columns, "wkb")) { printf("%12llu", wkb); }
      if (contains(columns, "mc")) { printf("%12llu", mc); }
      if (contains(columns, "mu")) { printf("%12llu", mu); }
      if (contains(columns, "mup")) { printf("%12llu", mup); }
      if (contains(columns, "mf")) { printf("%12llu", mf); }
      if (contains(columns, "mfp")) { printf("%12llu", mfp); }

      Ro += ro;
      Rob += rob;
      Rkb += rkb;
      Wo += wo;
      Wob += wob;
      Wkb += wkb;
      Mc += mc;
      Mu += mu;
      Mf += mf;
    }

    uint64_t Mup = (100 * Mu) / Mc;
    uint64_t Mfp = (100 * Mf) / Mc;

    if (contains(columns, "Ro")) { printf("%12llu", Ro); }
    if (contains(columns, "Rob")) { printf("%12llu", Rob); }
    if (contains(columns, "Rkb")) { printf("%12llu", Rkb); }
    if (contains(columns, "Wo")) { printf("%12llu", Wo); }
    if (contains(columns, "Wob")) { printf("%12llu", Wob); }
    if (contains(columns, "Wkb")) { printf("%12llu", Wkb); }
    if (contains(columns, "Mc")) { printf("%12llu", Mc); }
    if (contains(columns, "Mu")) { printf("%12llu", Mu); }
    if (contains(columns, "Mup")) { printf("%12llu", Mup); }
    if (contains(columns, "Mf")) { printf("%12llu", Mf); }
    if (contains(columns, "Mfp")) { printf("%12llu", Mfp); }

    printf("\n");
  }

  return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
