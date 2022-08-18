#Create maven artifact
FROM  maven:3.6.3-jdk-8 AS build

COPY . /usr/src/app
WORKDIR /usr/src/app
RUN mvn clean
RUN mvn package

#Copy built artifact from build into final image
FROM adoptopenjdk/openjdk8

EXPOSE 8086

RUN mkdir -p /opt/application/mdm/

RUN ln -sf /usr/share/zoneinfo/America/New_York /etc/localtime

COPY --from=build /usr/src/app/target/mdm-*.jar ./mdm.jar

ENTRYPOINT ["java","-jar","mdm.jar"]
