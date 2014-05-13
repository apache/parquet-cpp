THRIFT_INCLUDE=/home/nong/Projects/CDHImpala/thirdparty/thrift-0.9.0/build/include
LIB_DIR=bin/libs
LINK_FLAGS = -L/home/nong/Projects/CDHImpala/thirdparty/thrift-0.9.0/build/libthrift.a
CFLAGS=-g -Wall -fPIC -I./src -I./generated -I$(THRIFT_INCLUDE)

all: thrift compute_stats

thrift:
	mkdir -p generated
	thrift --gen cpp -o generated parquet-format/parquet.thrift 

parquet_types.o: generated/gen-cpp/parquet_types.cpp
	mkdir -p $(LIB_DIR)
	c++ $(CFLAGS) -c generated/gen-cpp/parquet_types.cpp -o $(LIB_DIR)/parquet_types.o

parquet.o: src/parquet.cc
	mkdir -p $(LIB_DIR)
	c++ $(CFLAGS) -c src/parquet.cc -o $(LIB_DIR)/parquet.o

example_util.o: example/example_util.cc
	mkdir -p $(LIB_DIR)
	c++ $(CFLAGS) -c example/example_util.cc -o $(LIB_DIR)/example_util.o

compute_stats: parquet_types.o parquet.o example_util.o
	mkdir -p bin
	c++ $(CFLAGS) $(LINK_FLAGS) $(LIB_DIR)/parquet.o $(LIB_DIR)/example_util.o example/compute_stats.cc -o bin/compute_stats
clean:

