RAMCLOUD_OBJ_DIR := $(RAMCLOUD_HOME)/obj.nanolog-integration

TARGETS :=  TableDownloader \
            TableUploader \
            SnapshotLoader \
            TimeOp \
            TimeReads \
            TimeMultiReads \
            TimeTransactionsAsyncReads \
            TableEnumeratorTestCase \
            TimeTraceTxReadOp \
            TransactionsTestCase \
            GetStats \
            GetMetrics \
            rcstat \
	    rcperf \
            TableImageSplitter \
	    ImageFileHashPartitioner \
	    ImageFileStats \
	    TableCreator

all: $(TARGETS)

%: src/main/cpp/%.cc
	g++ -o $@ $^ $(RAMCLOUD_OBJ_DIR)/OptionParser.o -g -std=c++0x -I$(RAMCLOUD_HOME)/src -I$(RAMCLOUD_HOME)/NanoLog/runtime -I$(RAMCLOUD_OBJ_DIR) -L$(RAMCLOUD_OBJ_DIR) -lramcloud -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto 

clean:
	rm -f $(TARGETS)
