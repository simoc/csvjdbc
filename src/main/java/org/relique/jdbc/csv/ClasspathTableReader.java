package org.relique.jdbc.csv;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import org.relique.io.TableReader;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;

public class ClasspathTableReader implements TableReader
{
	private final String path;
	private final String charset;
	private String fileExtension;

	public ClasspathTableReader(String path, String charset)
	{
		this.path = path;
		this.charset = charset;
	}

	public void setExtension(String fileExtension)
	{
		this.fileExtension = fileExtension;
	}

	public String getPath()
	{
		return path;
	}

	@Override
	public Reader getReader(Statement statement, String tableName) throws SQLException
	{
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(
				path + "/" + tableName + fileExtension);
		if (inputStream == null)
		{
			throw new SQLException(CsvResources.getString("tableNotFound") + ": " + tableName);
		}
		try
		{
			return charset != null ? new InputStreamReader(inputStream, charset) : new InputStreamReader(inputStream);
		}
		catch (UnsupportedEncodingException e)
		{
			throw new SQLException(e);
		}
	}

	@Override
	public List<String> getTableNames(Connection connection) throws SQLException
	{
		try (ScanResult scanResult = new ClassGraph().acceptPaths(path).scan())
		{
			return scanResult.getResourcesWithExtension(fileExtension.substring(1))
					.getPaths()
					.stream()
					.map(p -> p.substring(path.length() + 1, p.length() - fileExtension.length()))
					.filter(p -> !p.contains("/"))
					.collect(Collectors.toList());
		}
	}
}
