#!/bin/bash
echo "This script will check your current system for Fortis compatability:"
echo
echo "Validating java......-"

if type -p java; then
    echo found java executable in PATH
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    echo found java executable in JAVA_HOME     
    _java="$JAVA_HOME/bin/java"
else
    echo "java was not found on your system"
fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo version "$version"
    if [[ "$version" > "1.8" ]]; then
        echo version is more than 1.8
    else         
        echo version is less than 1.8
    fi
fi
#---------------------------------------------------------------------------------
echo
echo "Validating jdk......-"

if type -p javac; then
    echo found javac executable in PATH
    _javac=javac
elif [[ -n "$JAVAC_HOME" ]] && [[ -x "$JAVAC_HOME/bin/javac" ]];  then
    echo found javac executable in JAVAC_HOME     
    _javac="$JAVAC_HOME/bin/javac"
else
    echo "javac was not found on your system"
fi

if [[ "$_javac" ]]; then
    version=$("$_javac" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo version "$version"
    if [[ "$version" > "1.8" ]]; then
        echo version is more than 1.8
    else         
        echo version is less than 1.8
    fi
fi

#----------------------------------------------------------------------------------

echo
echo "Validating maven......-"

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
    version=$("$_mvn" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo version "$version"
    if [[ "$version" > "1.8" ]]; then
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
    echo "node was not found on your system"
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