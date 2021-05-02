# COMP4321 Search Engine Project - Spring 2021

# Team Members:
  1. KWOK, Ue Nam   (SID:20597580)
  2. YOON, Han Byul (SID:20385808)
  3. TSUI, Yuen Yau (SID:20607488)

# Compilation

  1. type "cd apache-tomcat-10.0.5/webapps/example"
  
  2. Compile the java file by following command:
      - javac -d bin -sourcepath src -cp ./lib/rocksdbjni-6.18.0-linus64.jar:./lib/jsoup-1.13.1.jar ./src/DbHandler.java ./src/Porter.java ./src/StopStem.java ./src/InvertedIndex.java ./src/Crawler.java
      - javac -d WEB-INF/classes -sourcepath src -cp ./lib/rocksdbjni-6.18.0-linus64.jar:./lib/jsoup-1.13.1.jar ./src/DbHandler.java ./src/Porter.java ./src/StopStem.java ./src/InvertedIndex.java ./src/Crawler.java

# Run the Crawler

  1. Run the java file by following command:
      - java -cp bin:./lib/rocksdbjni-6.18.0-linus64.jar:./lib/jsoup-1.13.1.jar mypackage/Crawler
