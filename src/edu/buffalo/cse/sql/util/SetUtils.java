/**
 * Standard implementations of assorted set operations.  These are provided for
 * your convenience.  You MAY, but should not need to modify this class.
 */

package edu.buffalo.cse.sql.util;

import java.util.Set;
import java.util.HashSet;

public class SetUtils {
  public static <T> Set<T> union(Set<T> setA, Set<T> setB) {
    Set<T> tmp = new HashSet<T>(setA);
    tmp.addAll(setB);
    return tmp;
  }

  public static <T> Set<T> intersect(Set<T> setA, Set<T> setB) {
    Set<T> tmp = new HashSet<T>();
    for (T x : setA)
      if (setB.contains(x))
        tmp.add(x);
    return tmp;
  }

  public static <T> Set<T> diff(Set<T> setA, Set<T> setB) {
    Set<T> tmp = new HashSet<T>(setA);
    tmp.removeAll(setB);
    return tmp;
  }
}