Parquet-cpp
===========
A C++ library to read parquet files.

To build you will need some version of boost installed and thrift 0.7+ installed.  
(If you are building thrift from source, you will need to set the THRIFT_HOME env
variable to the directory containing include/ and lib/.)

Then run:
<br>
<code>
cmake . 
</code>
<br>
<code>
make
</code>

The binaries will be built to ./bin which contains the libraries to link against as
well as a few example executables.
