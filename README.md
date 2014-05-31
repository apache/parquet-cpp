Parquet-cpp
===========
A C++ library to read parquet files.

To build you will need some version of boost installed and thrift 0.7+ installed.  
(If you are building thrift from source, you will need to set the THRIFT_HOME env
variable to the directory containing include/ and lib/.)

Then run:
<br>
<code>
thirdparty/download_thirdparty.sh
</code>
<br>
<code>
thirdparty/build_thirdparty.sh
</code>
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

Incremental builds can be done afterwords with just <code> make </code>.

Design
========
The library consists of 3 layers that map to the 3 units in the parquet format. 

The first is the encodings which correspond to data pages. The APIs at this level 
return single values.

The second layer is the column reader which corresponds to column chunks. The APIs at 
this level return a triple: definition level, repetition level and value. It also handles 
reading pages, compression and managing encodings. 

The 3rd layer would handle reading/writing records.

Developer Notes
========
The project adheres to the google coding convention: 
http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml 
with two notable exceptions. We do not encourage anonymous namespaces and the line 
length is 90 characters.

The project prefers the use of C++ style memory management. new/delete should be used 
over malloc/free. new/delete should be avoided whenever possible by using stl/boost 
where possible. For example, scoped_ptr instead of explicit new/delete and using 
std::vector instead of allocated buffers. Currently, c++11 features are not used.

For error handling, this project uses exceptions.

In general, many of the APIs at the layers are interface based for extensibility. To 
minimize the cost of virtual calls, the APIs should be batch-centric. For example, 
encoding should operate on batches of values rather than a single value.
