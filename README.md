MasteringHadoop Samples Repository
==================================

A git repository for all the code samples that are present in the book "Mastering Hadoop".

The preface of the book has instructions on how to compile and execute the examples in the book. All the projects use Maven for compilation and dependency management. The `mainClass` values in the `pom.xml` have to be changed to include the main class of each example. The template `pom.xml` that can be used is available in each and every example project. 

To compile, run the following commands in the project directory:
```
mvn compile
mvn assembly:single
```

* MasteringHadoop project covers the samples in chapters 2,3,4, and 5.
* MasteringYarn project covers the samples in chapter 6.
* MasteringStormOnYarn project covers the samples in chapter 7.
* MasteringHDFSReplacements project covers the sample in chapter 9.



