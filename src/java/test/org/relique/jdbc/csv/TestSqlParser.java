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
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package test.org.relique.jdbc.csv;

import org.relique.jdbc.csv.SqlParser;
import junit.framework.*;

/**This class is used to test the SqlParser class.
 *
 * @author Jonathan Ackerman
 * @version $Id: TestSqlParser.java,v 1.2 2002/01/01 23:04:26 jackerm Exp $
 */
public class TestSqlParser extends TestCase
{
  public TestSqlParser(String name)
  {
    super(name);
  }

  public void testParser()
  {
    try
    {
      SqlParser parser = new SqlParser();

      parser.parse("SELECT FLD_A,FLD_B, TEST, H FROM test");
      assertTrue("Incorrect table name",parser.getTableName().equals("test"));

      String[] cols = parser.getColumnNames();
      assertTrue("Incorrect Column Count",cols.length==4);

      assertTrue("Incorrect Column Name Col 0",cols[0].equals("FLD_A"));
      assertTrue("Incorrect Column Name Col 1",cols[1].equals("FLD_B"));
      assertTrue("Incorrect Column Name Col 2",cols[2].equals("TEST"));
      assertTrue("Incorrect Column Name Col 3",cols[3].equals("H"));

    }
    catch(Exception e)
    {
      fail("Unexpected Exception:" + e);
    }
  }
}