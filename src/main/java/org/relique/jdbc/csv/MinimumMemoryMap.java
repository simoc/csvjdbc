/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2015  Simon Chenery
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Array based implementation of the Map interface.
 * This implementation uses the minimum amount of memory to
 * store keys and values. However, inserts and lookups
 * take linear time O(N).
 */
public class MinimumMemoryMap<K, V> implements Map<K, V>
{
	ArrayList<K> keys;
	ArrayList<V> values;

	public MinimumMemoryMap()
	{
		keys = new ArrayList<K>();
		values = new ArrayList<V>();
	}

	public MinimumMemoryMap(int initialSize)
	{
		keys = new ArrayList<K>(initialSize);
		values = new ArrayList<V>(initialSize);
	}

	public MinimumMemoryMap(Map<K, V> otherMap)
	{
		keys = new ArrayList<K>();
		values = new ArrayList<V>();
		putAll(otherMap);
	}

	@Override
	public void clear()
	{
		keys.clear();
		values.clear();
	}

	@Override
	public int size()
	{
		return keys.size();
	}

	@Override
	public boolean isEmpty()
	{
		return keys.isEmpty();
	}

	@Override
	public boolean containsKey(Object key)
	{
		return keys.contains(key);
	}

	@Override
	public boolean containsValue(Object value)
	{
		return values.contains(value);
	}

	@Override
	public V get(Object key)
	{
		int index = keys.indexOf(key);
		if (index >= 0)
			return values.get(index);
		else
			return null;
	}

	@Override
	public V put(K key, V value)
	{
		V previousValue = null;
		int index = keys.indexOf(key);
		if (index >= 0)
		{
			previousValue = values.get(index);
			values.set(index, value);
		}
		else
		{
			keys.add(key);
			values.add(value);
		}
		return previousValue;
	}

	@Override
	public V remove(Object key)
	{
		V removedValue = null;
		int index = keys.indexOf(key);
		if (index >= 0)
		{
			removedValue = values.get(index);
			keys.remove(index);
			values.remove(index);
		}
		return removedValue;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m)
	{
		int requiredCapacity = size() + m.size();
		keys.ensureCapacity(requiredCapacity);
		values.ensureCapacity(requiredCapacity);

		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet())
		{
			put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public Set<K> keySet()
	{
		return new HashSet<K>(keys);
	}

	@Override
	public Collection<V> values()
	{
		return new ArrayList<V>(values);
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet()
	{
		int size = keys.size();
		Set<Map.Entry<K, V>> entrySet = new HashSet<Map.Entry<K, V>>(size);

		for (int index = 0; index < size; index++)
		{
			entrySet.add(new AbstractMap.SimpleEntry<K, V>(keys.get(index), values.get(index)));
		}
		return entrySet;
	}
}
