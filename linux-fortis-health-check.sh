#!/bin/bash
echo "Checking your current system for Fortis compatability:"
echo
echo "Validating java version and jdk..........................................-"

if type -p java; then
    echo found java executable in PATH
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    echo PASS: found java executable in JAVA_HOME     
    _java="$JAVA_HOME/bin/java"
else
    echo "java was not found on your machine"
fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo version: "$version"
    if [[ "$version" > "1.8" ]]; then
        echo PASS: version is more than 1.8
    else         
        echo WARN: version is less than 1.8
    fi
fi

if [[ "$_java" ]]; then
    versionJ=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $1}')
    echo your machine is running: "$versionJ"
    if [[ "$versionJ" == "java version " ]]; then
        echo WARN: java version is running the incorrect jre package, we recommend installing openJDK
    elif [[ "$versionJ" == "openjdk version " ]]; then
        echo PASS: you are running the correct openJDK
    else         
        echo WARN: detected unknown jdk package, please install openJDK
    fi
fi
echo "Validating jdk...........................................-"
if type -p javac; then
    echo PASS: found javac executable in PATH
    _javac=javac
elif [[ -n "$JAVAC_HOME" ]] && [[ -x "$JAVAC_HOME/bin/javac" ]];  then
    echo PASS: found javac executable in JAVAC_HOME     
    _javac="$JAVAC_HOME/bin/javac"
else
    echo FAIL: "javac was not found on your machine"
fi
#----------------------------------------------------------------------------------

echo
echo "Validating maven..........................................-"

if type -p mvn; then
    echo found maven executable in PATH
    _mvn=mvn
elif [[ -n "$MAVEN_HOME" ]] && [[ -x "$MAVEN_HOME/bin/mvn" ]];  then
    echo found mvn executable in MAVEN_HOME     
    _javac="$MAVEN_HOME/bin/mvn"
else
    echo "maven was not found on your system"
fi

if [[ "$_mvn" ]]; then
    version=$("$_mvn" -version 1>&1 | awk -F '"' '/Apache/ {print $0}')
    echo version "$version"
    versionNumb=$(echo "$version" 1>&1 | grep -o '\3(.*)')
    echo versionNumb "$versionNumb"
    if [[ "$version" < "1.8" ]]; then
        echo version is more than 1.8
    else         
        echo version is less than 1.8
    fi
fi

#----------------------------------------------------------------------------------

echo
echo "Validating node......-"

if type -p node; then
    echo found node executable in PATH
    _node=node
else
    echo "node was not found on your machine"
fi

if [[ "$_node" ]]; then
    version=$("$_node" -v 2>&1 | awk -F '"' '/version/ {print $2}')
    echo version "$version"
    if [[ "$version" > "1.8" ]]; then
        echo version is more than 1.8
    else         
        echo version is less than 1.8
    fi
fi