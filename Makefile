5300_Antelope: 5300_Antelope.o
	g++ -L/usr/local/db6/lib -o 5300_Antelope 5300_Antelope.o -ldb_cxx -lsqlparser

5300_Antelope.o : 5300_Antelope.cpp
	g++ -I/usr/local/db6/include -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -o "5300_Antelope.o" "5300_Antelope.cpp"
