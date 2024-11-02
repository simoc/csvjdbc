/**
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2008  Mario Frasca
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class BinaryOperation extends Expression
{
	private static final long MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;
	String operation;
	char op;
	Expression left, right;
	public BinaryOperation(String operation, Expression left, Expression right)
	{
		this.operation = operation;
		this.op = operation.charAt(0);
		this.left = left;
		this.right = right;
	}
	@Override
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object leftEval = left.eval(env);
		Object rightEval = right.eval(env);

		if (leftEval == null || rightEval == null)
			return null;

		try
		{
			Integer leftInt;
			BigInteger bil;
			boolean isLongExpression = false;

			if (leftEval instanceof Short)
			{
				leftInt = Integer.valueOf(((Short)leftEval).intValue());
				bil = new BigInteger(leftInt.toString());
			}
			else if (leftEval instanceof Long)
			{
				bil = new BigInteger(leftEval.toString());
				isLongExpression = true;
			}
			else
			{
				leftInt = (Integer)leftEval;
				bil = new BigInteger(leftInt.toString());
			}
			Integer rightInt;
			BigInteger bir;
			if (rightEval instanceof Short)
			{
				rightInt = Integer.valueOf(((Short)rightEval).intValue());
				bir = new BigInteger(rightInt.toString());
			}
			else if (rightEval instanceof Long)
			{
				bir = new BigInteger(rightEval.toString());
				isLongExpression = true;
			}
			else
			{
				rightInt = (Integer)rightEval;
				bir = new BigInteger(rightInt.toString());
			}
			if (op == '+')
				bil = bil.add(bir);
			else if (op == '-')
				bil = bil.subtract(bir);
			else if (op == '*')
				bil = bil.multiply(bir);
			else if (op == '/')
				bil = bil.divide(bir);
			else if (op == '%')
				bil = bil.remainder(bir);
			if (isLongExpression)
				return Long.valueOf(bil.toString());
			else
				return Integer.valueOf(bil.toString());
		}
		catch (ClassCastException e)
		{
		}
		catch (ArithmeticException e)
		{
			/* probably a divide by zero */
			throw new SQLException(e.getMessage());
		}

		try
		{
			Number leftN = (Number)leftEval;
			BigDecimal bdl = new BigDecimal(leftN.toString());
			Number rightN = (Number)rightEval;
			BigDecimal bdr = new BigDecimal(rightN.toString());
			if (op == '+')
				return Double.valueOf(bdl.add(bdr).toString());
			if (op == '-')
				return Double.valueOf(bdl.subtract(bdr).toString());
			if (op == '*')
				return Double.valueOf(bdl.multiply(bdr).toString());
			MathContext mc = new MathContext("precision=14 roundingMode=HALF_UP");
			if (op == '/')
				return Double.valueOf(bdl.divide(bdr, mc.getPrecision(), mc.getRoundingMode()).toString());
			if (op == '%')
				return Double.valueOf(bdl.remainder(bdr, mc).toString());
		}
		catch (ClassCastException e)
		{
		}
		catch (ArithmeticException e)
		{
			/* probably a divide by zero */
			throw new SQLException(e.getMessage());
		}

		try
		{
			if (op == '+' && leftEval instanceof Date)
			{
				Date leftD = (Date)leftEval;
				if (rightEval instanceof Time)
				{
					Time rightT = (Time)rightEval;
					Expression stringConverter = new ColumnName(StringConverter.COLUMN_NAME);
					StringConverter sc = (StringConverter) stringConverter.eval(env);
					return sc.parseTimestamp(leftD + " " + rightT);
				}
				else
				{
					Long rightLong;
					if (rightEval instanceof Short)
						rightLong = Long.valueOf(((Short)rightEval).longValue());
					else if (rightEval instanceof Long)
						rightLong = (Long)rightEval;
					else
						rightLong = Long.valueOf(((Integer)rightEval).intValue());
	  				return incrementDate(leftD, rightLong.longValue());
				}
			}
			else if (op == '+' && rightEval instanceof Date)
			{
				Date rightD = (Date)rightEval;
				if (leftEval instanceof Time)
				{
					Time leftT = (Time)leftEval;
					Expression stringConverter = new ColumnName(StringConverter.COLUMN_NAME);
					StringConverter sc = (StringConverter) stringConverter.eval(env);
					return sc.parseTimestamp(rightD + " " + leftT);
				}
				else
				{
					Long leftLong;
					if (leftEval instanceof Short)
						leftLong = Long.valueOf(((Short)leftEval).intValue());
					else if (leftEval instanceof Long)
						leftLong = (Long)leftEval;
					else
						leftLong = Long.valueOf(((Integer)leftEval).intValue());
					return incrementDate(rightD, leftLong.longValue());
				}
			}
			else if (op == '-' && leftEval instanceof Date && rightEval instanceof Long)
			{
				return incrementDate((Date)leftEval, -((Long)rightEval).longValue());
			}
			else if (op == '-' && leftEval instanceof Date && rightEval instanceof Integer)
			{
				return incrementDate((Date)leftEval, -((Integer)rightEval).intValue());
			}
			else if (op == '-' && leftEval instanceof Date && rightEval instanceof Short)
			{
				return incrementDate((Date)leftEval, -((Short)rightEval).intValue());
			}
			else if (op == '-' && (leftEval instanceof Date || rightEval instanceof Date))
			{
				if (!(leftEval instanceof Date))
				{
					Expression stringConverter = new ColumnName(StringConverter.COLUMN_NAME);
					StringConverter sc = (StringConverter) stringConverter.eval(env);
					leftEval = sc.parseDate(leftEval.toString());
				}
				if (!(rightEval instanceof Date))
				{
					Expression stringConverter = new ColumnName(StringConverter.COLUMN_NAME);
					StringConverter sc = (StringConverter) stringConverter.eval(env);
					rightEval = sc.parseDate(rightEval.toString());
	  			}
				if (leftEval != null && rightEval != null)
				{
					long nMillis = ((Date)leftEval).getTime() - ((Date)(rightEval)).getTime();
					long nDays = (nMillis + MILLISECONDS_PER_DAY / 2) / MILLISECONDS_PER_DAY;
					return Integer.valueOf((int)nDays);
				}
			}
		}
		catch (ClassCastException e)
		{
		}
		try
		{
			if (op == '+' || op == '-')
			{
				Timestamp leftD = (Timestamp)leftEval;
				long time = leftD.getTime();
				Number rightN = (Number)rightEval;
				BigDecimal bdr = new BigDecimal(rightN.toString());
				if (op == '+')
					return new Timestamp(time + bdr.longValue());
				if (op == '-')
					return new Timestamp(time - bdr.longValue());
			}
		}
		catch (ClassCastException e)
		{
			if ((op == '+' || op == '-') && leftEval instanceof Time)
			{
				Time leftT = (Time)leftEval;

				if (rightEval instanceof Number)
				{
					Number rightN = (Number)rightEval;
					long rightMillis = rightN.longValue();
					if (op == '-')
						rightMillis = -rightMillis;
					Time result = Time.valueOf(leftT.toLocalTime().plus(rightMillis, ChronoUnit.MILLIS));
					return result;
				}
				if (op == '-' && rightEval instanceof Time)
				{
					Time rightT = (Time)rightEval;
					return Long.valueOf(leftT.getTime() - rightT.getTime());
				}
			}
		}
		if(op == '+' || op == '|')
			return ""+leftEval+rightEval;
		return null;
	}
	private Date incrementDate(Date date, long nDays)
	{
		long newTime = date.getTime() +
		nDays * MILLISECONDS_PER_DAY + MILLISECONDS_PER_DAY / 2;
		Date newDate = new Date(newTime);
		/* Remove any time component from calculated date */
		newDate = Date.valueOf(newDate.toString());
		return newDate;
	}
	@Override
	public String toString()
	{
		return ""+operation+" "+left+" "+right;
	}
	@Override
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<>();
		result.addAll(left.usedColumns(availableColumns));
		result.addAll(right.usedColumns(availableColumns));
		return result;
	}
	@Override
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<>();
		result.addAll(left.aggregateFunctions());
		result.addAll(right.aggregateFunctions());
		return result;
	}
	@Override
	public boolean isValid()
	{
		/*
		 * An operation containing a logical expression such as (A > 5) + 1
		 * is not allowed in SQL.
		 */
		if (left instanceof LogicalExpression || right instanceof LogicalExpression)
			return false;
		else
			return (left.isValid() && right.isValid());
	}
}
