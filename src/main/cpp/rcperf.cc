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

#include <cmath>
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
    int replicas;
    std::string configFilename;

    // Set line buffering for stdout so that printf's and log messages
    // interleave properly.
    setvbuf(stdout, NULL, _IOLBF, 1024);

    // need external context to set log levels with OptionParser
    Context context(false);

    OptionsDescription clientOptions("rcperf");
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

        ("replicas",
         ProgramOptions::value<int>(&replicas),
         "Number of replicas configured for given RAMCloud cluster.")
        ("config",
         ProgramOptions::value<std::string>(&configFilename),
         "Configuration file for experiment specification.");
    
    OptionParser optionParser(clientOptions, argc, argv);
    context.transportManager->setSessionTimeout(
            optionParser.options.getSessionTimeout());

    LOG(NOTICE, "Connecting to %s",
        optionParser.options.getCoordinatorLocator().c_str());

    string locator = optionParser.options.getExternalStorageLocator();
    if (locator.size() == 0) {
        locator = optionParser.options.getCoordinatorLocator();
    }

    RamCloud client(&optionParser.options);

    // Default values for experiment parameters
    uint32_t key_range_start = 30;
    uint32_t key_range_end = 30;
    uint32_t key_points = 1;
    std::string key_points_mode = "linear";
    uint32_t value_range_start = 100;
    uint32_t value_range_end = 100;
    uint32_t value_points = 1;
    std::string value_points_mode = "linear";
    uint32_t dss_range_start = 100;
    uint32_t dss_range_end = 100;
    uint32_t dss_points = 1;
    std::string dss_points_mode = "linear";
    uint32_t multi_range_start = 32;
    uint32_t multi_range_end = 32;
    uint32_t multi_points = 1;
    std::string multi_points_mode = "linear";
    uint32_t server_range_start = 1;
    uint32_t server_range_end = 1;
    uint32_t server_points = 1;
    std::string server_points_mode = "linear";
    uint32_t samples_per_point = 1000;


    std::ifstream cfgFile(configFilename);
    std::string line;
    std::string op;
    while (true) {
      if (cfgFile.eof()) {
        printf("End of experiments\n");
        return 0;
      }

      bool foundCfg = false;
      while (std::getline(cfgFile, line)) {
        if (line[0] == '[') {
          op = line.substr(1, line.find_last_of(']') - 1);
          foundCfg = true;
        } else if (line[0] == '#') {
          // skip
          continue;
        } else if (line.size() == 0) {
          if (foundCfg == false)
            continue;
          else
            break;
        } else {
          std::string var_name = line.substr(0, line.find_first_of(' '));
          
          if (var_name.compare("key_range_start") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            key_range_start = var_int_value;
          } else if (var_name.compare("key_range_end") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            key_range_end = var_int_value;
          } else if (var_name.compare("key_points") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            key_points = var_int_value;
          } else if (var_name.compare("key_points_mode") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            key_points_mode = var_value;
          } else if (var_name.compare("value_range_start") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            value_range_start = var_int_value;
          } else if (var_name.compare("value_range_end") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            value_range_end = var_int_value;
          } else if (var_name.compare("value_points") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            value_points = var_int_value;
          } else if (var_name.compare("value_points_mode") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            value_points_mode = var_value;
          } else if (var_name.compare("dss_range_start") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            dss_range_start = var_int_value;
          } else if (var_name.compare("dss_range_end") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            dss_range_end = var_int_value;
          } else if (var_name.compare("dss_points") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            dss_points = var_int_value;
          } else if (var_name.compare("dss_points_mode") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            dss_points_mode = var_value;
          } else if (var_name.compare("multi_range_start") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            multi_range_start = var_int_value;
          } else if (var_name.compare("multi_range_end") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            multi_range_end = var_int_value;
          } else if (var_name.compare("multi_points") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            multi_points = var_int_value;
          } else if (var_name.compare("multi_points_mode") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            multi_points_mode = var_value;
          } else if (var_name.compare("server_range_start") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            server_range_start = var_int_value;
          } else if (var_name.compare("server_range_end") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            server_range_end = var_int_value;
          } else if (var_name.compare("server_points") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            server_points = var_int_value;
          } else if (var_name.compare("server_points_mode") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            server_points_mode = var_value;
          } else if (var_name.compare("samples_per_point") == 0) {
            std::string var_value = line.substr(line.find_last_of(' ') + 1, line.length());
            uint32_t var_int_value = std::stoi(var_value);
            samples_per_point = var_int_value;
          } else {
            printf("ERROR: Unknown parameter: %s\n", var_name.c_str());
            return 1;
          }
        }
      }

      if (foundCfg == false) {
        printf("End of experiments\n");
        return 0;
      }

      std::vector<uint32_t> key_sizes; 
      std::vector<uint32_t> value_sizes; 
      std::vector<uint32_t> dss_sizes; 
      std::vector<uint32_t> multi_sizes;
      std::vector<uint32_t> server_sizes;

      if (key_points > 1) {
        if (key_points_mode.compare("linear") == 0) {
          uint32_t step_size = 
            (key_range_end - key_range_start) / (key_points - 1);

          for (int i = key_range_start; i <= key_range_end; i += step_size) 
            key_sizes.push_back(i);
        } else if (key_points_mode.compare("geometric") == 0) {
          double c = pow(10, log10((double)key_range_end/(double)key_range_start) / (double)(key_points - 1));
          for (int i = key_range_start; i <= key_range_end; i *= c)
            key_sizes.push_back(i);
        } else {
          printf("ERROR: Unknown points mode: %s\n", key_points_mode.c_str());
          return 1;
        }
      } else {
        key_sizes.push_back(key_range_start);
      }

      if (value_points > 1) {
        if (value_points_mode.compare("linear") == 0) {
          uint32_t step_size = 
            (value_range_end - value_range_start) / (value_points - 1);

          for (int i = value_range_start; i <= value_range_end; i += step_size) 
            value_sizes.push_back(i);
        } else if (value_points_mode.compare("geometric") == 0) {
          double c = pow(10, log10((double)value_range_end/(double)value_range_start) / (double)(value_points - 1));
          for (int i = value_range_start; i <= value_range_end; i *= c)
            value_sizes.push_back(i);
        } else {
          printf("ERROR: Unknown points mode: %s\n", value_points_mode.c_str());
          return 1;
        }
      } else {
        value_sizes.push_back(value_range_start);
      }

      if (dss_points > 1) {
        if (dss_points_mode.compare("linear") == 0) {
          uint32_t step_size = 
            (dss_range_end - dss_range_start) / (dss_points - 1);

          for (int i = dss_range_start; i <= dss_range_end; i += step_size) 
            dss_sizes.push_back(i);
        } else if (dss_points_mode.compare("geometric") == 0) {
          double c = pow(10, log10((double)dss_range_end/(double)dss_range_start) / (double)(dss_points - 1));
          for (int i = dss_range_start; i <= dss_range_end; i *= c)
            dss_sizes.push_back(i);
        } else {
          printf("ERROR: Unknown points mode: %s\n", dss_points_mode.c_str());
          return 1;
        }
      } else {
        dss_sizes.push_back(dss_range_start);
      }

      if (multi_points > 1) {
        if (multi_points_mode.compare("linear") == 0) {
          uint32_t step_size = 
            (multi_range_end - multi_range_start) / (multi_points - 1);

          for (int i = multi_range_start; i <= multi_range_end; i += step_size) 
            multi_sizes.push_back(i);
        } else if (multi_points_mode.compare("geometric") == 0) {
          double c = pow(10, log10((double)multi_range_end/(double)multi_range_start) / (double)(multi_points - 1));
          for (int i = multi_range_start; i <= multi_range_end; i *= c)
            multi_sizes.push_back(i);
        } else {
          printf("ERROR: Unknown points mode: %s\n", multi_points_mode.c_str());
          return 1;
        }
      } else {
        multi_sizes.push_back(multi_range_start);
      }

      if (server_points > 1) {
        if (server_points_mode.compare("linear") == 0) {
          uint32_t step_size = 
            (server_range_end - server_range_start) / (server_points - 1);

          for (int i = server_range_start; i <= server_range_end; i += step_size) 
            server_sizes.push_back(i);
        } else if (server_points_mode.compare("geometric") == 0) {
          double c = pow(10, log10((double)server_range_end/(double)server_range_start) / (double)(server_points - 1));
          for (int i = server_range_start; i <= server_range_end; i *= c)
            server_sizes.push_back(i);
        } else {
          printf("ERROR: Unknown points mode: %s\n", server_points_mode.c_str());
          return 1;
        }
      } else {
        server_sizes.push_back(server_range_start);
      }

      if (op.compare("read") == 0) {
        uint64_t tableId = client.createTable("test");

        for (int ks_idx = 0; ks_idx < key_sizes.size(); ks_idx++) {
          uint32_t key_size = key_sizes[ks_idx];

          // Open data file for writing.
          FILE * datFile;
          char filename[128];
          sprintf(filename, "read.spp_%d.ks_%d.csv", samples_per_point, key_size);
          datFile = fopen(filename, "w");
          fprintf(datFile, "%12s %12s %12s %12s %12s %12s %12s %12s %12s %12s %12s %12s\n", 
              "ValueSize",
              "1th",
              "2th",
              "5th",
              "10th",
              "25th",
              "50th",
              "75th",
              "90th",
              "95th",
              "98th",
              "99th");

          for (int vs_idx = 0; vs_idx < value_sizes.size(); vs_idx++) {
            uint32_t value_size = value_sizes[vs_idx];
            printf("Read Test: key_size: %dB, value_size: %dB\n", key_size, value_size);

            char randomKey[key_size];
            char randomValue[value_size];

            client.write(tableId, randomKey, key_size, randomValue, value_size);

            Buffer value;
            uint64_t latency[samples_per_point];
            for (int i = 0; i < samples_per_point; i++) {
              bool exists;
              uint64_t start = Cycles::rdtsc();
              client.read(tableId, randomKey, key_size, &value, NULL, NULL, &exists);
              uint64_t end = Cycles::rdtsc();
              latency[i] = Cycles::toNanoseconds(end-start);
            }

            std::vector<uint64_t> latencyVec(latency, latency+samples_per_point);

            std::sort(latencyVec.begin(), latencyVec.end());

            uint64_t sum = 0;
            for (int i = 0; i < samples_per_point; i++) {
              sum += latencyVec[i];
            }

            fprintf(datFile, "%12d %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f\n", 
                value_size,
                latencyVec[samples_per_point*1/100]/1000.0,
                latencyVec[samples_per_point*2/100]/1000.0,
                latencyVec[samples_per_point*5/100]/1000.0,
                latencyVec[samples_per_point*10/100]/1000.0,
                latencyVec[samples_per_point*25/100]/1000.0,
                latencyVec[samples_per_point*50/100]/1000.0,
                latencyVec[samples_per_point*75/100]/1000.0,
                latencyVec[samples_per_point*90/100]/1000.0,
                latencyVec[samples_per_point*95/100]/1000.0,
                latencyVec[samples_per_point*98/100]/1000.0,
                latencyVec[samples_per_point*99/100]/1000.0);
            fflush(datFile);
          }

          fclose(datFile);
        }

        client.dropTable("test");
      } else if (op.compare("write") == 0) {
        uint64_t tableId = client.createTable("test");

        for (int ks_idx = 0; ks_idx < key_sizes.size(); ks_idx++) {
          uint32_t key_size = key_sizes[ks_idx];

          // Open data file for writing.
          FILE * datFile;
          char filename[128];
          sprintf(filename, "write.spp_%d.rf_%d.ks_%d.csv", samples_per_point, replicas, key_size);
          datFile = fopen(filename, "w");
          fprintf(datFile, "%12s %12s %12s %12s %12s %12s %12s %12s %12s %12s %12s %12s\n", 
              "ValueSize",
              "1th",
              "2th",
              "5th",
              "10th",
              "25th",
              "50th",
              "75th",
              "90th",
              "95th",
              "98th",
              "99th");

          for (int vs_idx = 0; vs_idx < value_sizes.size(); vs_idx++) {
            uint32_t value_size = value_sizes[vs_idx];
            printf("Write Test: key_size: %dB, value_size: %dB\n", key_size, value_size);

            char randomKey[key_size];
            char randomValue[value_size];

            client.write(tableId, randomKey, key_size, randomValue, value_size);

            uint64_t latency[samples_per_point];
            for (int i = 0; i < samples_per_point; i++) {
              bool exists;
              uint64_t start = Cycles::rdtsc();
              client.write(tableId, randomKey, key_size, randomValue, value_size);
              uint64_t end = Cycles::rdtsc();
              latency[i] = Cycles::toNanoseconds(end-start);
            }

            std::vector<uint64_t> latencyVec(latency, latency+samples_per_point);

            std::sort(latencyVec.begin(), latencyVec.end());

            uint64_t sum = 0;
            for (int i = 0; i < samples_per_point; i++) {
              sum += latencyVec[i];
            }

            fprintf(datFile, "%12d %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f\n", 
                value_size,
                latencyVec[samples_per_point*1/100]/1000.0,
                latencyVec[samples_per_point*2/100]/1000.0,
                latencyVec[samples_per_point*5/100]/1000.0,
                latencyVec[samples_per_point*10/100]/1000.0,
                latencyVec[samples_per_point*25/100]/1000.0,
                latencyVec[samples_per_point*50/100]/1000.0,
                latencyVec[samples_per_point*75/100]/1000.0,
                latencyVec[samples_per_point*90/100]/1000.0,
                latencyVec[samples_per_point*95/100]/1000.0,
                latencyVec[samples_per_point*98/100]/1000.0,
                latencyVec[samples_per_point*99/100]/1000.0);
            fflush(datFile);
          }

          fclose(datFile);
        }

        client.dropTable("test");
      } else if (op.compare("multiread") == 0) {
        for (int sv_idx = 0; sv_idx < server_sizes.size(); sv_idx++) {
          uint32_t server_size = server_sizes[sv_idx];

          uint64_t tableId = client.createTable("test", server_size);

          for (int ms_idx = 0; ms_idx < multi_sizes.size(); ms_idx++) {
            uint32_t multi_size = multi_sizes[ms_idx];

            for (int ks_idx = 0; ks_idx < key_sizes.size(); ks_idx++) {
              uint32_t key_size = key_sizes[ks_idx];

              // Open data file for writing.
              FILE * datFile;
              char filename[128];
              sprintf(filename, "multiread.spp_%d.sv_%d.mrs_%d.ks_%d.csv", samples_per_point, server_size, multi_size, key_size);
              datFile = fopen(filename, "w");
              fprintf(datFile, "%12s %12s %12s %12s %12s %12s %12s %12s %12s %12s %12s %12s\n", 
                  "ValueSize",
                  "1th",
                  "2th",
                  "5th",
                  "10th",
                  "25th",
                  "50th",
                  "75th",
                  "90th",
                  "95th",
                  "98th",
                  "99th");

              // Construct keys.
              char keys[multi_size][key_size];
              memset(keys, 0, multi_size * key_size);
              for (int i = 0; i < multi_size; i++) {
                sprintf(keys[i], "%d", i);
              }
                
              for (int vs_idx = 0; vs_idx < value_sizes.size(); vs_idx++) {
                uint32_t value_size = value_sizes[vs_idx];
                printf("Multiread Test: server_size: %d, multi_size: %d, key_size: %dB, value_size: %dB\n", server_size, multi_size, key_size, value_size);

                // Write value_size data into objects.
                for (int i = 0; i < multi_size; i++) {
                  char randomValue[value_size];
                  client.write(tableId, keys[i], key_size, randomValue, value_size);
                }

                // Prepare multiread data structures.
                MultiReadObject requestObjects[multi_size];
                MultiReadObject* requests[multi_size];
                Tub<ObjectBuffer> values[multi_size];

                for (int i = 0; i < multi_size; i++) {
                  requestObjects[i] = MultiReadObject(tableId, keys[i], 
                      key_size, &values[i]);
                  requests[i] = &requestObjects[i];
                }

                uint64_t latency[samples_per_point];
                for (int i = 0; i < samples_per_point; i++) {
                  uint64_t start = Cycles::rdtsc();
                  client.multiRead(requests, multi_size);
                  uint64_t end = Cycles::rdtsc();
                  latency[i] = Cycles::toNanoseconds(end-start);
                }

                std::vector<uint64_t> latencyVec(latency, latency+samples_per_point);

                std::sort(latencyVec.begin(), latencyVec.end());

                uint64_t sum = 0;
                for (int i = 0; i < samples_per_point; i++) {
                  sum += latencyVec[i];
                }

                fprintf(datFile, "%12d %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f\n", 
                    value_size,
                    latencyVec[samples_per_point*1/100]/1000.0,
                    latencyVec[samples_per_point*2/100]/1000.0,
                    latencyVec[samples_per_point*5/100]/1000.0,
                    latencyVec[samples_per_point*10/100]/1000.0,
                    latencyVec[samples_per_point*25/100]/1000.0,
                    latencyVec[samples_per_point*50/100]/1000.0,
                    latencyVec[samples_per_point*75/100]/1000.0,
                    latencyVec[samples_per_point*90/100]/1000.0,
                    latencyVec[samples_per_point*95/100]/1000.0,
                    latencyVec[samples_per_point*98/100]/1000.0,
                    latencyVec[samples_per_point*99/100]/1000.0);
                fflush(datFile);
              } // vs_idx

              fclose(datFile);
            } // ks_idx
          } // ms_idx

          client.dropTable("test");
        } // sv_idx
      } else if (op.compare("multiread_fixeddss") == 0) {
        for (int sv_idx = 0; sv_idx < server_sizes.size(); sv_idx++) {
          uint32_t server_size = server_sizes[sv_idx];

          uint64_t tableId = client.createTable("test", server_size);

          for (int dss_idx = 0; dss_idx < dss_sizes.size(); dss_idx++) {
            uint32_t dss_size = dss_sizes[dss_idx];

            // Open data file for writing.
            FILE * datFile;
            char filename[128];
            sprintf(filename, "multiread_fixeddss.spp_%d.sv_%d.dss_%d.csv", samples_per_point, server_size, dss_size);
            datFile = fopen(filename, "w");
            fprintf(datFile, "%12s %12s %12s %12s %12s %12s %12s %12s %12s %12s %12s %12s\n", 
                "MultiSize",
                "1th",
                "2th",
                "5th",
                "10th",
                "25th",
                "50th",
                "75th",
                "90th",
                "95th",
                "98th",
                "99th");

            for (int ms_idx = 0; ms_idx < multi_sizes.size(); ms_idx++) {
              uint32_t multi_size = multi_sizes[ms_idx];

              printf("Multiread Fixed DSS Test: server_size: %d, dss_size: %d, multi_size: %d\n", server_size, dss_size, multi_size);

              // Compute key_size and value_size.
              uint32_t key_size = 30; // Use fixed 30B keys.
              uint32_t value_size = (dss_size - key_size) / multi_size;

              // Construct keys.
              char keys[multi_size][key_size];
              memset(keys, 0, multi_size * key_size);
              for (int i = 0; i < multi_size; i++) {
                sprintf(keys[i], "%d", i);
              }

              // Write value_size data into objects.
              for (int i = 0; i < multi_size; i++) {
                char randomValue[value_size];
                client.write(tableId, keys[i], key_size, randomValue, value_size);
              }

              // Prepare multiread data structures.
              MultiReadObject requestObjects[multi_size];
              MultiReadObject* requests[multi_size];
              Tub<ObjectBuffer> values[multi_size];

              for (int i = 0; i < multi_size; i++) {
                requestObjects[i] = MultiReadObject(tableId, keys[i], 
                    key_size, &values[i]);
                requests[i] = &requestObjects[i];
              }

              uint64_t latency[samples_per_point];
              for (int i = 0; i < samples_per_point; i++) {
                uint64_t start = Cycles::rdtsc();
                client.multiRead(requests, multi_size);
                uint64_t end = Cycles::rdtsc();
                latency[i] = Cycles::toNanoseconds(end-start);
              }

              std::vector<uint64_t> latencyVec(latency, latency+samples_per_point);

              std::sort(latencyVec.begin(), latencyVec.end());

              uint64_t sum = 0;
              for (int i = 0; i < samples_per_point; i++) {
                sum += latencyVec[i];
              }

              fprintf(datFile, "%12d %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f\n", 
                  multi_size,
                  latencyVec[samples_per_point*1/100]/1000.0,
                  latencyVec[samples_per_point*2/100]/1000.0,
                  latencyVec[samples_per_point*5/100]/1000.0,
                  latencyVec[samples_per_point*10/100]/1000.0,
                  latencyVec[samples_per_point*25/100]/1000.0,
                  latencyVec[samples_per_point*50/100]/1000.0,
                  latencyVec[samples_per_point*75/100]/1000.0,
                  latencyVec[samples_per_point*90/100]/1000.0,
                  latencyVec[samples_per_point*95/100]/1000.0,
                  latencyVec[samples_per_point*98/100]/1000.0,
                  latencyVec[samples_per_point*99/100]/1000.0);
              fflush(datFile);
            } // ms_idx

            fclose(datFile);
          } // dss_idx

          client.dropTable("test");
        } // sv_idx
      } else {
        printf("ERROR: Unknown operation: %s\n", op.c_str());
        return 1;
      }
    }
  
    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
