# ==========================================
# STAGE 1: Build the Scala Project using Maven
# ==========================================
FROM maven:3.8.6-eclipse-temurin-11 AS builder

# Set working directory
WORKDIR /app

# Step 1: Copy only the pom.xml first to cache dependencies (saves time!)
COPY pom.xml .
RUN mvn dependency:go-offline

# Step 2: Copy the actual Scala code
COPY src/ /app/src/

# Step 3: Compile the Scala code into a "Fat JAR"
RUN mvn clean package

# ==========================================
# STAGE 2: Run the Pipeline in a Spark Container
# ==========================================
FROM bitnami/spark:3.3.0

WORKDIR /app

# Copy the compiled JAR from the builder stage
COPY --from=builder /app/target/shelf-architect-1.0-SNAPSHOT.jar /app/shelf-architect.jar

# Command to execute the Scala Spark job
CMD ["spark-submit", \
     "--class", "com.niq.shelf.ShelfOptimizer", \
     "--master", "local[*]", \
     "/app/shelf-architect.jar"]
