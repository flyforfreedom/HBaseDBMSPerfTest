# HBaseRDBMSPerfTest

A simple application that can generate a predefined put, get and scan set of operations for the purpose of
comparing the performance of an RDBMS and a NoSQL data store.

Any number of changes can be made to the tool, but the basic operation is the Two classes that implement the TestDriver interface.
One of these classes tests and RDBMS (initially Teradata) the other a NoSQL (initially HBase). These two classes can be tweaked to support
other data stores as needed.

The methods in the TestDriver.java interface definition contain javadoc describing what they should do. Sample code may be levereged from
the two instances that are implemented (TestHBaseDriver and TestRDBMSDriver).

This project was created in NetBeans - so if you wish to extend the GUI selecting NetBeans as your IDE will allow you to use the drag and drop
GUI builder intereface. However, any IDE could be used to modify this project.

Note that it is a Maven project.

