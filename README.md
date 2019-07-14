# 5300-Antelope

Sprint Verano: Vishakha Bhavsar, Virmel Gacad

Sprint Verano:

  Milestone 1: Skeleton
  
     Sets up the Berkeley DB Environment
     Prompts and interacts with user
     Rudimentary functions
     Unparsing handled
     
  Milestone 2: Rudimentary Storage Engine:
    
      This program varied in output and we could not pinpoint where the problem came from. I, Virmel, could compile the program 
      but could not execute the test_heap_storage() function stored in the heap_storage.cpp/h files. Vishakha on the other hand 
      was able to run the program and have it execute the sql5300.cpp file all the way to the test_heap_storage() function.
      She was able to troubleshoot all the way till the project() method. For the select() method, handle is not being handled
      properly, which is attributing to failing the test.
      
 heap_storage.cpp is where most of our implementation lies.
 sql5300.cpp is the driver of the files.
 There is nothing special about the Makefile. It is the exact one that the professor gave us.
 Other files include:
 heap_storage.h
 storage_engine.h
 
Sprint Otono
    
