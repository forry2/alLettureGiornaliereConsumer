FROM openjdk:11
COPY target/alLettureGiornaliereConsumer-0.0.1.jar /
ENTRYPOINT ["java","-jar","/alLettureGiornaliereConsumer-0.0.1.jar"]