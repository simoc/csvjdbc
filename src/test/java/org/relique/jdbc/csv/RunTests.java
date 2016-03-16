/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2001  Jonathan Ackerman

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/
package org.relique.jdbc.csv;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestSqlParser.class,
	TestCsvDriver.class,
	TestDbfDriver.class,
	TestScrollableDriver.class,
	TestFileSetInputStream.class,
	TestJoinedTables.class,
	TestCryptoFilter.class,
	TestPrepareStatement.class,
	TestStringConverter.class,
	TestZipFiles.class,
	TestAggregateFunctions.class,
	TestOrderBy.class,
	TestGroupBy.class,
	TestLimitOffset.class,
	TestFixedWidthFiles.class,
	TestDoubleQuoting.class,
	TestSubQuery.class
})

/**
 * Junit4 test suite for the CsvJdbc driver.
 *
 * @author Jonathan Ackerman
 */
public class RunTests
{
}
