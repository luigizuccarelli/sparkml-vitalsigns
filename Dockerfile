FROM combustml/mleap-spring-boot:0.17.0-SNAPSHOT
RUN mkdir /models && chmod 777 /models
# ensure the vitalsign.model.zip is in the build directory 
COPY build/vitalsign.model.zip /models/
