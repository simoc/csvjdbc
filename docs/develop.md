##Building From Source

To checkout and build the latest source code from the
[CsvJdbc git repository](http://sourceforge.net/p/csvjdbc/code/ci/master/tree/),
use the following commands ([git](http://git-scm.com/) and
[Maven](http://maven.apache.org/) must first be installed).

    cd $HOME
    mkdir workspace1
    cd workspace1
    git clone git://git.code.sf.net/p/csvjdbc/code csvjdbc
    cd csvjdbc
    mvn install
    cd target
    dir csvjdbc*.jar

##Working With Eclipse

1. Start Eclipse with workspace `workspace1`
2. Install the JavaCC Eclipse Plug-in from the Help ->
Eclipse Marketplace... menu option
3. Create a new project named `csvjdbc` using menu option
File -> New -> Java Project
4. Open `src/main/javacc/org/relique/jdbc/csv/where.jj` in the Eclipse
Editor and then select menu option JavaCC -> Compile with javacc

##Maven Project Usage

CsvJdbc is available at [Maven Central](http://search.maven.org/).
To include CsvJdbc in a [Maven](http://maven.apache.org/) project,
add the following lines to the `pom.xml` file.

    <project>
     ...
    
      <dependencies>
        <dependency>
          <groupId>net.sourceforge.csvjdbc</groupId>
          <artifactId>csvjdbc</artifactId>
          <version>1.0.35</version>
        </dependency>
      </dependencies>

##Contributing

A change to CsvJdbc must first be entered as a
[Bug](http://sourceforge.net/p/csvjdbc/bugs/)
or
[Feature Request](http://sourceforge.net/p/csvjdbc/feature-requests/)
before starting development.
The change will then be accepted or not
accepted by a CsvJdbc administrator.

Completed changes must be provided as a [git](http://www.git-scm.com)
pull request and include
a unit test to test the changed functionality.

Label *easy* is used to identify existing
[Easy Bugs](https://sourceforge.net/p/csvjdbc/bugs/search/?q=labels%3Aeasy)
and
[Easy Feature Requests](https://sourceforge.net/p/csvjdbc/feature-requests/search/?q=labels%3Aeasy)
that are suitable for new developers.
