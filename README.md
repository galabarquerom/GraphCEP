# GraphCEP

To install required tools to run the artifacts follow these steps:

- Download Scala IDE for Eclipse 4.7.0 from http://scala-ide.org/ and launch it with eclipse.exe (for Windows) or eclipse.app (for Mac).
- Install Maven plugin for Scala on Eclipse:
	* Click on Help-> Install New Software...
	* Select Add...
	* Write a name (for example 'Maven Scala').
	* Paste the following URL in 'Location' space: http://alchim31.free.fr/m2e-scala/update-site/
	* Press 'OK'.
	* Select 'Maven Integration for Eclipse' checkbox and click on Next button every time it appears until the installation finishes. If 'The installation cannot be completed as requested' message appears select 'Show original error and build my own solution' on the radiobutton and choose 'Install different version than originally requested' and 'Update items already installed'.
	* Restart Eclipse.

Note that our artifacts use Maven to build dependencies on them. Sometimes, when importing a Maven project, dependencies can be downloaded wrongly and to solve this problem it is necessary to follow these steps:
	- Right click on the Maven project.
	- Select Run as-> Maven clean.
	- Select Run as-> Maven install.
	- Update the project right clicking on Maven project and selecting Maven->Update project.

Some of our artifacts have two projects: a Scala project and a Java project. The first one contains the source code and the second one is an auxiliary project that permits to export a jar file.

# 1. Motorbike4Esper Project

Motorbike example with Esper technology.

This project sends a particular amount of simple events, selected from a configuration file, to an Esper engine and returns complex events built from simple events and the execution time. 

With this project we have obtained results shown in Table 1 - "Esper" row and Table 2 - "Esper" row of our paper.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

For running the project you will need:

- Internet connection
- Scala IDE for Eclipse (our tests have been run with the version described above)
- Java JDK 1.8

Note: 
### Installing

To install the project follow these steps:

1. Import Project into workspace.
2. Update maven dependencies (follow steps described above)
3. Open ‘config.properties’ located in ‘motorbikeProject’ project to set the configuration parameters:
	- NUM_EVENTS -> set the number of events to run a test
	- DELAY -> set to ‘TRUE’ if you want to test with a delay of 1 second between events (results Table 1 - Row 2) and ‘FALSE’ otherwise (results Table 2 - Row 2). 
Note that selecting DELAY to 'TRUE' will do that the execution takes the same amount of seconds as value of NUM_EVENTS.

## Running the tests

To run our experiments follow these steps:

- In ‘config.properties’ set ‘NUM_EVENTS’ to 5000, 10000, 20000 or 30000
- Set ‘DELAY’ to TRUE to test with a delay and set to FALSE otherwise
- Right click on Main.java file on ‘motorbikeProject’ located on src/main/java/com/cor/cep/ project: Run as -> Java Application

Results will be shown on the console.

## Deployment

To export a jar file:

- Right Click on ‘motorbikeProject’ project and select Export->Runnable JAR File->Next->Finish
- Note that to run the jar file you have to copy ‘motorbike.csv’ and ‘config.properties’ (of ‘motorbikeProject’ project) files in the same folder as the jar.

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management
* [Spring](https://spring.io/) - The framework used

## Authors

Gala Barquero, Lola Burgueño, Javier Troya, Antonio Vallecillo


# 2. Motorbike4Graphx Project

Motorbike example with Spark technology. 

This project sends a particular amount of simple events, selected from a configuration file, to a graph and returns complex events built from simple events and the execution time. 

With this project we have obtained results shown in Table 1 - "Spark" row and Table 2 - "Spark" row of our paper.

For this artifact we have two projects:

- motorbike4Graphx: the scala project with the source code.
- MotorbikeGraph: java project to export a jar file of the project.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

For running the project you will need:

- Internet connection
- Scala IDE for Eclipse (our tests have been run with the version described above)
- Java JDK 1.8


### Installing

To install out artifact follow these steps:

1. Import Project into workspace.
2. Update maven dependencies (follow steps described above)
3. Open ‘config.properties’ located in ‘MotorbikeGraph’ project to set the configuration parameters:
	- NUM_EVENTS -> set the number of events to run a test
	- DELAY -> set to ‘TRUE’ if you want to test with a delay of 1 second between events (results Table 1 - Row 3) and ‘FALSE’ otherwise (results Table 2 - Row 3). 
Note that selecting DELAY to 'TRUE' will do that our execution take the same amount of seconds as value of NUM_EVENTS.

## Running the tests

To run our experiments follow these steps:

- In ‘config.properties’ set ‘NUM_EVENTS’ to 5000, 10000, 20000 or 30000
- Set ‘DELAY’ to TRUE to test with a delay and set to FALSE otherwise
- Right click on Main.java file on ‘MotorbikeGraph’ project and located on src/main/java/com/cor/graphx: Run as -> Java Application

Results will be shown on the console. The two last log messages of MotorbikeEventGenerator class will show the execution time of sending all simple events selected in the configuration file and the start timestamp. Note that when the last simple event has been sent all complex events haven't been processed. To calculate the exact execution time of processing complex events it is necessary to substract the start timestamp to the timestamp of the last complex event produced (first value in the tuple of the last log message in the console).

Note that this project will never stop the execution in Eclipse because our queries run in an infinite loop, but the console will stop showing results when the last complex event has been produced.

## Deployment

To export a jar file:

- Right Click on ‘MotorbikeGraph’ project and select Export->Runnable JAR File->Next->Finish
- Note that to run the jar file you have to copy ‘motorbike.csv’ and ‘config.properties’ (of ‘MotorbikeGraph’ project) files in the same folder as the jar.

## Built With

* [Apache Spark - Graphx tool](https://spark.apache.org/docs/latest/graphx-programming-guide.html) - The technology used
* [Maven](https://maven.apache.org/) - Dependency Management
* [Scala](https://www.scala-lang.org/) - Coding Language

## Authors

Gala Barquero, Lola Burgueño, Javier Troya, Antonio Vallecillo


# 3. TwitterFlickrStatic Project

TwitterFlickr example with Spark technology. 

This project creates a graph with synthetic data from a configuration file, runs our queries five times and shows their execution times on the console to calculate the mean of execution time of each query.

With this project we have obtained results shown in Table 3 of our paper.

For this artifact we have two projects:

- TwitterFlickr: the scala project with the source code.
- TwitterFlickrStatic: java project to export a jar file of the project.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

For running the project you will need:

- Internet connection
- Scala IDE for Eclipse (our tests have been run with the version described above)
- Java JDK 1.8


### Installing

To install out artifact follow these steps:

1. Import Project into workspace.
2. Update maven dependencies (like described above)
3. Open ‘config.properties’ located in ‘TwitterFlickr’ project to set the configuration parameters:
	- numInactiveTwitterUsers: number of Inactive Twitter Users.
	- numInfluencerTwitterUsers: number of Influencer Twitter Users
	- numActiveTwitterUsers: number of Active Twitter Users
	- numFlickrUsers: number of Flickr Users
	- numHashtags: number of Hashtags

## Running the tests

To run our experiments follow these steps:

- In ‘config.properties’ set:
	* numInactiveTwitterUsers: 10000, 21000 or 44000
	* numInfluencerTwitterUsers: 5
	* numActiveTwitterUsers: 1000, 2100 or 4400
	* numFlickrUsers: 10000, 21000 or 44000
	* numHashtags: 100
- Right click on Main.java file on ‘TwitterFlickr’ project located on src: Run as -> Java Application

It is necessary to wait some minutes until the graph is created. Once the graph creation is completed, 5 execution time results will be shown on the console for each query. To obtain results from Table 3 we have calculated the mean with the last 3 results for each query.

## Deployment

To export a jar file:

- Right Click on ‘TwitterFlickr’ project and select Export->Runnable JAR File->Next->Finish
- Note that to run the jar file you have to copy ‘config.properties’ (of ‘TwitterFlickr’ project) file in the same folder as the jar.

## Built With

* [Apache Spark - Graphx tool](https://spark.apache.org/docs/latest/graphx-programming-guide.html) - The technology used
* [Maven](https://maven.apache.org/) - Dependency Management
* [Scala](https://www.scala-lang.org/) - Coding Language

## Authors

Gala Barquero, Lola Burgueño, Javier Troya, Antonio Vallecillo


# 4. TwitterFlickrStreamingDEMO Project

Just a DEMO of TwitterFlickr example CEP with Spark technology. First of all this project creates an initial graph and then starts a simulation of how our CEP architecture works, handling new edges and nodes and running the queries in an infinite loop.

Execution times of sending 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

For running the project you will need:

- Internet connection
- Scala IDE for Eclipse (our tests have been run with the version described above)
- Java JDK 1.8


### Installing

To install out artifact follow these steps:

1. Import Project into workspace.
2. Update maven dependencies (as described above)

## Running the tests

To run the DEMO: right click on Main.java file on ‘TwitterFlickr’ project located on src: Run as -> Java Application

Results will be shown on the console. Once the initial graph creation is completed, execution times of queries, node handling and edge handling will be shown.

## Deployment

To export a jar file:

- Right Click on ‘TwitterFlickr’ project and select Export->Runnable JAR File->Next->Finish
- Note that to run the jar file you have to copy ‘config.properties’ (of ‘TwitterFlickr’ project) file in the same folder as the jar.

## Built With

* [Apache Spark - Graphx tool](https://spark.apache.org/docs/latest/graphx-programming-guide.html) - The technology used
* [Maven](https://maven.apache.org/) - Dependency Management
* [Scala](https://www.scala-lang.org/) - Coding Language

## Authors

Gala Barquero, Lola Burgueño, Javier Troya, Antonio Vallecillo


