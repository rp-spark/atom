package utilities

import java.util.Date

class ExtendedString(s: String) {
  def isNumber: Boolean = s.matches("[+-]?\\d+.?\\d+")
}

object ExtendedString {
  implicit def String2ExtendedString(s: String) = new ExtendedString(s)
}
