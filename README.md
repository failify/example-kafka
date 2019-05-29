This repo contains sample [Failify](https://failify.io) test cases for Kafka. To make things easier for illustration
purposes, the required files from the build directory of v2.2.0 of Kafka built on Debian Stretch (using 
``./gradlew clean releaseTarGz -x signArchives`` command) is included in the repo
so anyone who wants to try the test cases is not required to build Kafka from scratch.

To run the test cases, first you need to install the following dependencies:
- Docker 1.13+

Also make sure you can run docker commands with the user you are running the tests with. For more information check
[Failify documentation](https://docs.failify.io).

After installing all the dependencies, you can run the test cases using:

```console
$ docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v $(pwd):/kafka -w /kafka -it maven:3.6.0-jdk-8 mvn -f failify/pom.xml test
```
