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

#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include <time.h>
#include <dirent.h>

#include <iostream>
#include <fstream>
#include <thread>

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
 * A set of per-thread loading statistics. Each loader thread continually
 * updates these statistics, while a statistics reporting thread with a
 * reference to each thread's stats regularly prints statistic summaries to the
 * screen.
 */
struct ThreadStats {
  /*
   * The total number of objects this thread has uploaded to RAMCloud.
   */
  long objectsLoaded = 0;

  /*
   * The total number of files this thread has uploaded.
   */
  long filesLoaded = 0;

  /*
   * The total number of files this thread has been given to load.
   */
  long totalFilesToLoad = 0;

  /*
   * The total number of bytes this thread has read from disk.
   */
  long bytesReadFromDisk = 0;

  /*
   * The total number of bytes (keys and values) this thread has written into
   * RAMCloud.
   */ 
  long bytesWrittenToRAMCloud = 0;
};

/**
 * A thread which reports statistics on the loader threads in the system at a
 * set interval. This thread gets information on each thread via a shared
 * ThreadStat struct with each thread.
 *
 * \param threadStats
 *      Array of all the shared ThreadStat structs, one per thread.
 * \param numThreads
 *      The number of loader threads.
 * \param reportInterval
 *      The number of seconds between reporting status to the screen.
 * \param formatString
 *      What columns of information to output to the screen.
 */
void statsReporterThread(struct ThreadStats *threadStats, int numThreads, 
    int reportInterval, std::string formatString) {

  int colWidth = 10;
  const char *colFormatStr = "%10s";

  // Print the column headers.
  for (int i = 0; i < numThreads; i++) {
    if (formatString.find("o") != std::string::npos) {
      printf(colFormatStr, (std::to_string(i) + ".o").c_str());
    }

    if (formatString.find("f") != std::string::npos) {
      printf(colFormatStr, (std::to_string(i) + ".f").c_str());
    }

    if (formatString.find("b") != std::string::npos) {
      printf(colFormatStr, (std::to_string(i) + ".b").c_str());
    }

    if (formatString.find("d") != std::string::npos) {
      printf(colFormatStr, (std::to_string(i) + ".d").c_str());
    }
  }

  if (formatString.find("O") != std::string::npos) {
    printf(colFormatStr, "O");
  }

  if (formatString.find("F") != std::string::npos) {
    printf(colFormatStr, "F");
  }

  if (formatString.find("B") != std::string::npos) {
    printf(colFormatStr, "B");
  }

  if (formatString.find("D") != std::string::npos) {
    printf(colFormatStr, "D");
  }

  if (formatString.find("T") != std::string::npos) {
    printf(colFormatStr, "T");
  }

  printf("\n");


  struct ThreadStats lastThreadStats[numThreads];
  memcpy(lastThreadStats, threadStats, numThreads*sizeof(struct ThreadStats));
 
  time_t startTime;
  time(&startTime); 
  while (true) {
    sleep(reportInterval);

    time_t currTime;
    time(&currTime);
    long timeElapsed = (long) difftime(currTime, startTime);

    long totalCurrObjRate = 0;
    long totalFilesLoaded = 0;
    long totalFilesToLoad = 0;
    long totalCurrReadRate = 0;
    long totalCurrWriteRate = 0;
    for (int i = 0; i < numThreads; i++) {
      struct ThreadStats *lastStats = &lastThreadStats[i];
      struct ThreadStats *currStats = &threadStats[i];

      long objectsLoaded = 
          currStats->objectsLoaded - lastStats->objectsLoaded;
      long bytesReadFromDisk = 
          currStats->bytesReadFromDisk - lastStats->bytesReadFromDisk;
      long byteWrittenToRAMCloud = currStats->bytesWrittenToRAMCloud 
          - lastStats->bytesWrittenToRAMCloud;

      long currObjRate = objectsLoaded / reportInterval;
      long currReadRate = bytesReadFromDisk / reportInterval;
      long currWriteRate = byteWrittenToRAMCloud / reportInterval;

      if (formatString.find("o") != std::string::npos) {
        printf(colFormatStr, std::to_string(currObjRate).c_str());
      }

      if (formatString.find("f") != std::string::npos) {
        char buf[colWidth];
        sprintf(buf, "(%d/%d)", currStats->filesLoaded, 
            currStats->totalFilesToLoad);
        printf(colFormatStr, buf);
      }

      if (formatString.find("b") != std::string::npos) {
        printf(colFormatStr, std::to_string(currWriteRate / 1000000l).c_str());
      }

      if (formatString.find("d") != std::string::npos) {
        printf(colFormatStr, std::to_string(currReadRate / 1000000l).c_str());
      }
      
      totalCurrObjRate += currObjRate;
      totalCurrReadRate += currReadRate;
      totalCurrWriteRate += currWriteRate;
      totalFilesLoaded += currStats->filesLoaded;
      totalFilesToLoad += currStats->totalFilesToLoad;
    }

    if (formatString.find("O") != std::string::npos) {
      printf(colFormatStr, std::to_string(totalCurrObjRate).c_str());
    }

    if (formatString.find("F") != std::string::npos) {
      char buf[colWidth];
      sprintf(buf, "(%d/%d)", totalFilesLoaded, totalFilesToLoad);
      printf(colFormatStr, buf);
    }

    if (formatString.find("B") != std::string::npos) {
      printf(colFormatStr, 
          std::to_string(totalCurrWriteRate / 1000000l).c_str());
    }

    if (formatString.find("D") != std::string::npos) {
      printf(colFormatStr, 
          std::to_string(totalCurrReadRate / 1000000l).c_str());
    }

    if (formatString.find("T") != std::string::npos) {
      printf(colFormatStr, std::to_string(timeElapsed/60l).c_str());
    }

    printf("\n");

    // Capture the current stats as last seen stats.
    memcpy(lastThreadStats, threadStats, 
        numThreads*sizeof(struct ThreadStats));

    if (totalFilesLoaded == totalFilesToLoad) {
      break;
    }
  }
}  
 

/**
 * A loader thread which takes a set of files to load and loads them 
 * sequentially.
 *
 * \param client
 *      RAMCloud client object to use for this thread. Each thread currently
 *      gets its own client object because the RAMCloud client object is not
 *      thread-safe.
 * \param fileList
 *      Master list of all the file names that compose the table.
 * \param startIndex
 *      The index in the load list at which this thread's loading work starts.
 * \param length
 *      The segment length in the load list for this thread.
 * \param multiwriteSize
 *      The size of multiwrites to use.
 */
void loaderThread(RamCloud *client, int serverSpan,
    std::vector<std::string> fileList, std::string snapshotDir, int startIndex, 
    int length, int multiwriteSize, struct ThreadStats *stats) {
 
  stats->totalFilesToLoad = length;

  printf("Starting LoaderThread: {startIndex: %u, length: %u, "
      "multiwriteSize: %u}\n", startIndex, length, multiwriteSize);

  for (int fIndex = startIndex; fIndex < startIndex + length; fIndex++) { 
    std::ifstream inFile;
    std::string fileName = snapshotDir + "/" + fileList[fIndex];

    inFile.open(fileName.c_str(), std::ios::binary);

    uint64_t tableId = client->createTable(
        fileName.substr(0, fileName.find(".img")).c_str(), serverSpan);

    Tub<MultiWriteObject> objects[multiwriteSize];
    MultiWriteObject* requests[multiwriteSize];
    
    std::vector<std::string> keys;
    std::vector<std::string> values;

    char lenBuffer[sizeof(uint32_t)];
    while(inFile.read(lenBuffer, sizeof(lenBuffer))) {
      uint32_t keyLength = *((uint32_t*) lenBuffer); 
      char keyBuffer[keyLength];
      inFile.read(keyBuffer, keyLength);
      keys.emplace_back((const char*)keyBuffer, keyLength);
      
      inFile.read(lenBuffer, sizeof(lenBuffer));
      uint32_t dataLength = *((uint32_t*) lenBuffer);
      char dataBuffer[dataLength];
      inFile.read(dataBuffer, dataLength);
      values.emplace_back((const char*)dataBuffer, dataLength);

      stats->bytesReadFromDisk += sizeof(lenBuffer)*2 + keyLength + dataLength;

      objects[keys.size() - 1].construct( tableId,
                                    (const void*) keys.back().data(),
                                    keyLength,
                                    (const void*) values.back().data(),
                                    dataLength );
      requests[keys.size() - 1] = objects[keys.size() - 1].get();

      stats->bytesWrittenToRAMCloud += keyLength + dataLength;

      if (keys.size() == multiwriteSize) {
        try {
          client->multiWrite(requests, multiwriteSize);
        } catch(RAMCloud::ClientException& e) {
          fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
          return;
        }
        
        stats->objectsLoaded += multiwriteSize;

        keys.clear();
        values.clear();
      }
    }

    if (keys.size() > 0) {
      try {
        client->multiWrite(requests, keys.size());
      } catch(RAMCloud::ClientException& e) {
        fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
        return;
      }

      stats->objectsLoaded += keys.size();

      keys.clear();
      values.clear();
    }

    inFile.close();

    stats->filesLoaded++;
  }
}

/** 
 * A utility for uploading a table into RAMCloud from a table image generated
 * by the TableDownloader utility.
 */
int
main(int argc, char *argv[])
try
{
  int clientIndex;
  int numClients;
  std::string snapshotDir;
  int serverSpan;
  int numThreads;
  int multiwriteSize;
  int reportInterval;
  std::string reportFormat;

  // Set line buffering for stdout so that printf's and log messages
  // interleave properly.
  setvbuf(stdout, NULL, _IOLBF, 1024);

  OptionsDescription clientOptions("SnapshotLoader");
  clientOptions.add_options()

    ("clientIndex",
     ProgramOptions::value<int>(&clientIndex)->
        default_value(0),
     "Index of this client (first client is 0) [default: 0].")
    ("numClients",
     ProgramOptions::value<int>(&numClients)->
        default_value(1),
     "Total number of clients running [default: 1].")

    ("snapshotDir",
     ProgramOptions::value<std::string>(&snapshotDir),
     "Directory where the snapshot is located.")
    ("serverSpan",
     ProgramOptions::value<int>(&serverSpan)->
         default_value(1),
     "Server span for the new table [default: 1].")
    ("numThreads",
     ProgramOptions::value<int>(&numThreads)->
         default_value(1),
     "Number of threads to use for uploading partitions in parallel. Only "
     "useful when image file has been partitioned. [default: 1]")
    ("multiwriteSize",
     ProgramOptions::value<int>(&multiwriteSize)->
         default_value(32),
     "Size of multiwrites to use to RAMCloud [default: 32].")
    ("reportInterval",
     ProgramOptions::value<int>(&reportInterval)->
         default_value(2),
     "Number of seconds between reporting status to the screen. [default: 2].")
    ("reportFormat",
     ProgramOptions::value<std::string>(&reportFormat)->
         default_value("OFBDT"),
     "Format options for status report output.\n"
     "  O - Total objects uploaded per second.\n"
     "  o - Per thread objects uploaded per second.\n"
     "  F - Total files uploaded.\n"
     "  f - Per thread files uploaded.\n"
     "  B - Total write bandwidth to RAMCloud in MB/s.\n"
     "  b - Per thread write bandwidth to RAMCloud in MB/s.\n"
     "  D - Total disk read bandwidth in MB/s.\n"
     "  d - Per thread disk read bandwidth in MB/s.\n"
     "  T - Total time elapsed.\n"
     "[default: OFBDT]");
  
  OptionParser optionParser(clientOptions, argc, argv);

  printf("SnapshotLoader: {numClients: %u, clientIndex: %u, numThreads: %u, "
      "serverSpan: %u, multiwriteSize: %u, reportInterval: %u, "
      "reportFormat: %s}\n", 
      numClients, clientIndex, numThreads, serverSpan, multiwriteSize, 
      reportInterval, reportFormat.c_str());

  std::string locator = optionParser.options.getExternalStorageLocator();
  if (locator.size() == 0) {
    locator = optionParser.options.getCoordinatorLocator();
  }

  std::vector<std::string> fileList;

  DIR *dpdf;
  struct dirent *epdf;
  dpdf = opendir(snapshotDir.c_str());
  if (dpdf != NULL) {
    while (epdf = readdir(dpdf)) {
      if (!(strcmp(epdf->d_name, ".") == 0 ||
         strcmp(epdf->d_name, "..") == 0)) {
        fileList.emplace_back(epdf->d_name);
      } 
    }
  }

  std::sort(fileList.begin(), fileList.end());

  printf("Found %u total files\n", fileList.size());

  /*
   * Calculate the segment of the list that this loader instance is
   * responsible for loading.
   */
  int q = fileList.size() / numClients;
  int r = fileList.size() % numClients;

  int loadSize;
  int loadOffset;
  if (clientIndex < r) {
    loadSize = q + 1;
    loadOffset = (q + 1) * clientIndex;
  } else {
    loadSize = q;
    loadOffset = ((q + 1) * r) + (q * (clientIndex - r));
  }

  /*
   * Divvy up the load and start the threads.
   */
  std::vector<std::thread> threads;
  RamCloud *clients[numThreads];
  ThreadStats tStats[numThreads];
  for (int i = 0; i < numThreads; i++) {
    int qt = loadSize / numThreads;
    int rt = loadSize % numThreads;

    int threadLoadSize;
    int threadLoadOffset;
    if (i < rt) {
      threadLoadSize = qt + 1;
      threadLoadOffset = (qt + 1) * i;
    } else {
      threadLoadSize = qt;
      threadLoadOffset = ((qt + 1) * rt) + (qt * (i - rt));
    }

    threadLoadOffset += loadOffset;

    clients[i] = new RamCloud(locator.c_str());

    threads.emplace_back(loaderThread, clients[i], serverSpan, fileList,
        snapshotDir, threadLoadOffset, threadLoadSize, multiwriteSize, 
        &tStats[i]);
  }

  // Give the threads some time to initialize their statistics. Otherwise the
  // stats thread will find that totalFileLoaded == totalFilesToLoad and exit.
  sleep(1);

  // Start statistics reporting thread.
  std::thread statsReporter(statsReporterThread, (struct ThreadStats*)tStats, 
      numThreads, reportInterval, reportFormat);

  for (int i = 0; i < numThreads; i++) {
    threads[i].join();
  }

  statsReporter.join();

  for (int i = 0; i < numThreads; i++) {
    delete clients[i];
  }

  return 0;
} catch (RAMCloud::ClientException& e) {
  fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
  return 1;
} catch (RAMCloud::Exception& e) {
  fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
  return 1;
}
