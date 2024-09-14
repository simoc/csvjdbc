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

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({
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
	TestArrayFunctions.class,
	TestRandomFunction.class,
	TestToNumberFunction.class,
	TestOrderBy.class,
	TestGroupBy.class,
	TestLimitOffset.class,
	TestLineNumber.class,
	TestFixedWidthFiles.class,
	TestDoubleQuoting.class,
	TestSubQuery.class,
	TestClasspathResources.class
})

/**
 * Junit5 test suite for the CsvJdbc driver.
 *
 * @author Jonathan Ackerman
 */
public class RunSuite
{
}
