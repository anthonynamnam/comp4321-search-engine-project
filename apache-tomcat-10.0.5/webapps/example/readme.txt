- bin/ destination of the compiled package
- lib/ libraries that the package relies on
- src/ source java files


under this folder, compile the source file by

javac -cp lib/* src/*.java -d bin -Xlint:deprecation


after that, you get the compiled package in bin/ and then you can
1. put everything under the lib folder to the WEB-INF/lib
2. remove the package folder (named mypackage in this example) into WEB-INF/classes