RAMCLOUD_DIR := $(HOME)/RAMCloud
RAMCLOUD_OBJ_DIR := $(RAMCLOUD_DIR)/obj.jtransactions

imager: src/imager.cc
	g++ -o $@ $^ $(RAMCLOUD_OBJ_DIR)/OptionParser.o -std=c++0x -I$(RAMCLOUD_DIR)/src -I$(RAMCLOUD_OBJ_DIR) -L$(RAMCLOUD_OBJ_DIR) -lramcloud -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto 
