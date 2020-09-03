## Releasing CsvJdbc

Use the following steps to create a CsvJdbc release.

1. Check environment

```
java -version
```

must be 1.6.

1. Checkout (replace simoc with your SourceForge username)

```
git clone ssh://simoc@git.code.sf.net/p/csvjdbc/code csvjdbc-code
git clone ssh://simoc@git.code.sf.net/p/csvjdbc/website csvjdbc-website
```

1. Check that XML `<property name="rel">` tag in file
`csvjdbc-code/build/build.xml` is the version number we are creating.
If not, update it and commit.

1. Compile

```
cd csvjdbc-code/build
ant jar test
```

1. Check that the ant test step above does not report failing unit tests

1. Upload to Sourceforge web site (replace `simoc` and `1.0-29` with values
for this release).

```
sftp simoc,csvjdbc@frs.sourceforge.net
cd /home/frs/project/c/cs/csvjdbc/CsvJdbc
mkdir 1.0-29
cd 1.0-29
put target/csvjdbc-1.0-29.jar
```

1.  Check that XML `<project><version>` tag in file `pom.xml` is the version
number we are creating plus "-SNAPSHOT" suffix. If not, update it and
commit.

1. Check that you have a `$HOME/.m2/settings.xml` file containing
the following XML tags, as described
[here](http://central.sonatype.org/pages/apache-maven.html)
and that the GPG profile contains a GPG key created using
[these steps](http://central.sonatype.org/pages/working-with-pgp-signatures.html)

```
<server><id>git.code.sf.net</id> ...
<server><id>ossrh</id> ...
<profile><id>ossrh</id> ... contains GPG profile
```

1. Maven deploy to Maven Central

```
mvn release:clean release:prepare
```

(accept defaults at prompts for CsvJdbc version, git tag, next CsvJdbc
version number. You are also prompted for Sourceforge password several
times).

```
mvn release:perform
```

1. Login to [Nexus Repository Manager](https://oss.sonatype.org/)
(username and password same as in `<ossrh>` XML tag in
`$HOME/.m2/settings.xml`), click on Staging Repositories in left panel,
then on row netsourceforgecsvjdbc- ..., then Close in toolbar and
Confirm in dialog box, then Release in toolbar and Confirm in dialog
box, as described
[here](http://central.sonatype.org/pages/releasing-the-deployment.html).

1. Update Tracker tickets from 'Pending' to 'Closed' with a comment
that they are included in release 1.0-29.

1. Create a `README.md` file listing the changed Tracker tickets and
upload to 1.0-29 directory, as in Step 7.

1. Upload new `index.html` if it has changed since last release.

```
cd csvjdbc-website/www
sftp simoc@web.sourceforge.net
cd /home/project-web/c/cs/csvjdbc/htdocs
put index.html
```

