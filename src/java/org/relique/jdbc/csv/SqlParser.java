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
package org.relique.jdbc.csv;

import java.util.Vector;
import java.util.StringTokenizer;

/**This is a very crude and SQL simple parser used by the Csv JDBC driver. It
 * only handles SELECT statements in the format "SELECT xxx,yyy,zzz FROM fffff"
 *
 * @author Jonathan Ackerman
 * @version $Id: SqlParser.java,v 1.1 2001/01/23 09:17:48 jackerm Exp $
 */
public class SqlParser
{
  public String tableName;
  public String[] columnNames;

  public void parse(String sql) throws Exception
  {
    tableName=null;
    columnNames=new String[0];

    String upperSql = sql.toUpperCase();

    if (!upperSql.startsWith("SELECT "))
      throw new Exception("Malformed SQL. Missing SELECT statement.");

    if (upperSql.lastIndexOf(" FROM ")==-1)
      throw new Exception("Malformed SQL. Missing FROM statement.");

    int fromPos = upperSql.lastIndexOf(" FROM ");

    tableName = sql.substring(fromPos+6).trim();

    Vector cols = new Vector();
    StringTokenizer tokenizer = new StringTokenizer(upperSql.substring(7,fromPos),",");

    while (tokenizer.hasMoreTokens())
     cols.add(tokenizer.nextToken().trim());

    columnNames = new String[cols.size()];
    cols.copyInto(columnNames);
  }

  public String getTableName()
  {
    return tableName;
  }

  public String[] getColumnNames()
  {
    return columnNames;
  }
}