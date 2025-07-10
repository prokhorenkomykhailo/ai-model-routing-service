#!/bin/bash

# JSON Test Application Build and Run Script

echo "=== Building JSON Test Application ==="

# Set variables
PROJECT_DIR="/home/beou/IdeaProjects/lucy/lucid-backend2/lucid-ai-routing-service"
SRC_DIR="$PROJECT_DIR/src/main/java"
BUILD_DIR="$PROJECT_DIR/build"
TEST_DATA_FILE="$PROJECT_DIR/test-data.txt"

# Create build directory
mkdir -p "$BUILD_DIR"

# Find Maven dependencies (assuming Maven is used)
if [ -f "$PROJECT_DIR/pom.xml" ]; then
    echo "Found Maven project, using Maven to compile..."
    cd "$PROJECT_DIR"
    
    # Compile the project
    mvn compile -q
    
    if [ $? -eq 0 ]; then
        echo "✓ Maven compilation successful"
        
        # Run the test application
        echo ""
        echo "=== Running JSON Test Application ==="
        echo "Test data file: $TEST_DATA_FILE"
        echo ""
        
        # Use Maven to run the test
        mvn exec:java -Dexec.mainClass="com.lucid.automation.airouting.test.JsonTestApp" -Dexec.args="$TEST_DATA_FILE" -q
        
    else
        echo "✗ Maven compilation failed"
        exit 1
    fi
else
    echo "No Maven project found, using javac directly..."
    
    # Try to find JAR files for classpath
    CLASSPATH=""
    if [ -d "$PROJECT_DIR/target/lib" ]; then
        CLASSPATH="$PROJECT_DIR/target/lib/*"
    elif [ -d "$HOME/.m2/repository" ]; then
        # Common Maven dependencies
        JACKSON_VERSION="2.15.2"
        SLF4J_VERSION="1.7.36"
        LOGBACK_VERSION="1.2.12"
        
        JACKSON_CORE="$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-core/$JACKSON_VERSION/jackson-core-$JACKSON_VERSION.jar"
        JACKSON_DATABIND="$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-databind/$JACKSON_VERSION/jackson-databind-$JACKSON_VERSION.jar"
        JACKSON_ANNOTATIONS="$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-annotations/$JACKSON_VERSION/jackson-annotations-$JACKSON_VERSION.jar"
        SLF4J_API="$HOME/.m2/repository/org/slf4j/slf4j-api/$SLF4J_VERSION/slf4j-api-$SLF4J_VERSION.jar"
        LOGBACK_CLASSIC="$HOME/.m2/repository/ch/qos/logback/logback-classic/$LOGBACK_VERSION/logback-classic-$LOGBACK_VERSION.jar"
        LOGBACK_CORE="$HOME/.m2/repository/ch/qos/logback/logback-core/$LOGBACK_VERSION/logback-core-$LOGBACK_VERSION.jar"
        
        CLASSPATH="$JACKSON_CORE:$JACKSON_DATABIND:$JACKSON_ANNOTATIONS:$SLF4J_API:$LOGBACK_CLASSIC:$LOGBACK_CORE"
    fi
    
    # Compile Java files
    echo "Compiling Java files..."
    find "$SRC_DIR" -name "*.java" | head -10  # Show first 10 files being compiled
    
    if [ -n "$CLASSPATH" ]; then
        javac -cp "$CLASSPATH" -d "$BUILD_DIR" $(find "$SRC_DIR" -name "*.java")
    else
        javac -d "$BUILD_DIR" $(find "$SRC_DIR" -name "*.java")
    fi
    
    if [ $? -eq 0 ]; then
        echo "✓ Compilation successful"
        
        # Run the test application
        echo ""
        echo "=== Running JSON Test Application ==="
        echo "Test data file: $TEST_DATA_FILE"
        echo ""
        
        if [ -n "$CLASSPATH" ]; then
            java -cp "$BUILD_DIR:$CLASSPATH" com.lucid.automation.airouting.test.JsonTestApp "$TEST_DATA_FILE"
        else
            java -cp "$BUILD_DIR" com.lucid.automation.airouting.test.JsonTestApp "$TEST_DATA_FILE"
        fi
    else
        echo "✗ Compilation failed"
        exit 1
    fi
fi

echo ""
echo "=== Test Complete ==="
