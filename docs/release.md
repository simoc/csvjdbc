## Releasing CsvJdbc

Use the following steps to create a CsvJdbc release.

* Check environment

```
java -version
```

must be 11.

* Checkout from GitHub

Push all changes from GitHub git repository to SourceForge repository
(replace `simoc` with your GitHub and SourceForge usernames).

```
git clone https://github.com/simoc/csvjdbc.git csvjdbc-github
cd csvjdbc-github
git push ssh://simoc@git.code.sf.net/p/csvjdbc/code master
```

* Checkout from SourceForge

```
git clone ssh://simoc@git.code.sf.net/p/csvjdbc/code csvjdbc-code
git clone ssh://simoc@git.code.sf.net/p/csvjdbc/website csvjdbc-website
```

* Check that XML `<project><version>` tag in file `pom.xml` is the version
number we are creating plus "-SNAPSHOT" suffix. If not, update it and
commit.

* Check that you have a `$HOME/.m2/settings.xml` file containing
the following XML tags, as described
[here](http://central.sonatype.org/pages/apache-maven.html)
and that the GPG profile contains a GPG key created using
[these steps](http://central.sonatype.org/pages/working-with-pgp-signatures.html)

```
<server><id>git.code.sf.net</id> ...
<server><id>ossrh</id> ...
<profile><id>ossrh</id> ... contains GPG profile
```

* Maven deploy to Maven Central

```
mvn release:clean release:prepare
```

(accept defaults at prompts for CsvJdbc version, git tag, next CsvJdbc
version number. You are also prompted for Sourceforge password several
times).

* Check that the Maven step above does not report failing unit tests

```
mvn release:perform
```

* Login to [Nexus Repository Manager](https://oss.sonatype.org/)
(username and password same as in `<ossrh>` XML tag in
`$HOME/.m2/settings.xml`), click on Staging Repositories in left panel,
then on row netsourceforgecsvjdbc- ..., then Close in toolbar and
Confirm in dialog box. This may take 20 minutes to process. Then click
Release in toolbar and Confirm in dialog
box, as described
[here](http://central.sonatype.org/pages/releasing-the-deployment.html).

* Upload to Sourceforge web site (replace `simoc`, `1.0-29` and `1.0.29` with values
for this release).

```
sftp simoc,csvjdbc@frs.sourceforge.net
cd /home/frs/project/c/cs/csvjdbc/CsvJdbc
mkdir 1.0-29
cd 1.0-29
put target/csvjdbc-1.0.29.jar
```

* Create a `README.md` file listing the changed SourceForge
Tracker tickets and GitHub issues and
upload to 1.0-29 directory, as in previous step.

* Update Tracker tickets from 'Pending' to 'Closed' with a comment
that they are included in release 1.0-29.

* Upload new `index.html` if it has changed since last release.

```
cd csvjdbc-website/www
sftp simoc@web.sourceforge.net
cd /home/project-web/c/cs/csvjdbc/htdocs
put index.html
```

* Push the commits and tag created by Maven to the GitHub repository

```
git push https://github.com/simoc/csvjdbc.git master
git push https://github.com/simoc/csvjdbc.git csvjdbc-1.0.29
```
