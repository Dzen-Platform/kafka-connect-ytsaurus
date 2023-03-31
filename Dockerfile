# WARNING: THIS IS A BUILDER ONLY

# Use Amazon Corretto 17 as the base image
FROM amazoncorretto:17

# Set the working directory
WORKDIR /app

# Copy the Gradle wrapper and its properties file into the container
COPY gradlew gradlew
COPY gradle/wrapper/gradle-wrapper.properties gradle/wrapper/gradle-wrapper.properties
COPY gradle/wrapper/gradle-wrapper.jar gradle/wrapper/gradle-wrapper.jar

# Copy the build script
COPY build.gradle build.gradle
COPY settings.gradle settings.gradle
COPY versions.gradle versions.gradle

# Copy the source code
COPY src/ src/

# Grant execute permissions to the Gradle wrapper
RUN chmod +x ./gradlew

# Build the project
RUN ./gradlew shadowJar
