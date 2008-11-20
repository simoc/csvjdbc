package org.relique.jdbc.csv;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class Expression {
	public Object eval(Map env) {
		return null;
	}
	public List usedColumns() {
		return null;
	}
}
