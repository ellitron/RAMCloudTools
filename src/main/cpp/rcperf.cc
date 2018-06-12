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
    RamCloud client(&context, locator.c_str(),
            optionParser.options.getClusterName().c_str());

    // Default values for experiment parameters
    uint32_t key_range_start = 30;
    uint32_t key_range_end = 30;
    uint32_t key_points = 1;
    std::string key_points_mode = "linear";
    uint32_t value_range_start = 100;
    uint32_t value_range_end = 100;
    uint32_t value_points = 1;
    std::string value_points_mode = "linear";
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
    while (std::getline(cfgFile, line)) {
      if (line[0] == '[') {
        op = line.substr(1, line.find_last_of(']') - 1);
      } else if (line[0] == '#') {
        // skip
      } else if (line.size() == 0) {
        // end of this operation's configuration
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

    if (op.compare("read") == 0) {
      std::vector<uint32_t> key_sizes; 
      std::vector<uint32_t> value_sizes; 

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

      for (int i = 0; i < key_sizes.size(); i++) {
        printf("key_size: %d\n", key_sizes[i]);
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

      for (int i = 0; i < value_sizes.size(); i++) {
        printf("value_size: %d\n", value_sizes[i]);
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
