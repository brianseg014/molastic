from jnius import autoclass

Appendable = autoclass("java.lang.Appendable")
ArithmeticException = autoclass("java.lang.ArithmeticException")
ArrayIndexOutOfBoundsException = autoclass(
    "java.lang.ArrayIndexOutOfBoundsException"
)
ArrayStoreException = autoclass("java.lang.ArrayStoreException")
Boolean = autoclass("java.lang.Boolean")
Byte = autoclass("java.lang.Byte")
CharSequence = autoclass("java.lang.CharSequence")
Character = autoclass("java.lang.Character")
ClassCastException = autoclass("java.lang.ClassCastException")
ClassNotFoundException = autoclass("java.lang.ClassNotFoundException")
CloneNotSupportedException = autoclass("java.lang.CloneNotSupportedException")
Comparable = autoclass("java.lang.Comparable")
Double = autoclass("java.lang.Double")
Enum = autoclass("java.lang.Enum")
EnumConstantNotPresentException = autoclass(
    "java.lang.EnumConstantNotPresentException"
)
Exception = autoclass("java.lang.Exception")
Float = autoclass("java.lang.Float")
IllegalAccessException = autoclass("java.lang.IllegalAccessException")
IllegalArgumentException = autoclass("java.lang.IllegalArgumentException")
IllegalMonitorStateException = autoclass(
    "java.lang.IllegalMonitorStateException"
)
IllegalStateException = autoclass("java.lang.IllegalStateException")
IllegalThreadStateException = autoclass(
    "java.lang.IllegalThreadStateException"
)
IndexOutOfBoundsException = autoclass("java.lang.IndexOutOfBoundsException")
InstantiationException = autoclass("java.lang.InstantiationException")
Integer = autoclass("java.lang.Integer")
InterruptedException = autoclass("java.lang.InterruptedException")
Iterable = autoclass("java.lang.Iterable")
Long = autoclass("java.lang.Long")
Math = autoclass("java.lang.Math")
NegativeArraySizeException = autoclass("java.lang.NegativeArraySizeException")
NoSuchFieldException = autoclass("java.lang.NoSuchFieldException")
NoSuchMethodException = autoclass("java.lang.NoSuchMethodException")
NullPointerException = autoclass("java.lang.NullPointerException")
Number = autoclass("java.lang.Number")
NumberFormatException = autoclass("java.lang.NumberFormatException")
Object = autoclass("java.lang.Object")
ReflectiveOperationException = autoclass(
    "java.lang.ReflectiveOperationException"
)
RuntimeException = autoclass("java.lang.RuntimeException")
SecurityException = autoclass("java.lang.SecurityException")
Short = autoclass("java.lang.Short")
StackTraceElement = autoclass("java.lang.StackTraceElement")
StrictMath = autoclass("java.lang.StrictMath")
String = autoclass("java.lang.String")
StringBuffer = autoclass("java.lang.StringBuffer")
StringBuilder = autoclass("java.lang.StringBuilder")
StringIndexOutOfBoundsException = autoclass(
    "java.lang.StringIndexOutOfBoundsException"
)
System = autoclass("java.lang.System")
TypeNotPresentException = autoclass("java.lang.TypeNotPresentException")
UnsupportedOperationException = autoclass(
    "java.lang.UnsupportedOperationException"
)
Void = autoclass("java.lang.Void")

BigDecimal = autoclass("java.math.BigDecimal")
BigInteger = autoclass("java.math.BigInteger")
MathContext = autoclass("java.math.MathContext")
RoundingMode = autoclass("java.math.RoundingMode")

Annotation = autoclass("java.text.Annotation")
AttributedCharacterIterator = autoclass(
    "java.text.AttributedCharacterIterator"
)
AttributedString = autoclass("java.text.AttributedString")
Bidi = autoclass("java.text.Bidi")
BreakIterator = autoclass("java.text.BreakIterator")
CharacterIterator = autoclass("java.text.CharacterIterator")
ChoiceFormat = autoclass("java.text.ChoiceFormat")
CollationElementIterator = autoclass("java.text.CollationElementIterator")
CollationKey = autoclass("java.text.CollationKey")
Collator = autoclass("java.text.Collator")
DateFormat = autoclass("java.text.DateFormat")
DateFormat = autoclass("java.text.DateFormat")
DateFormatSymbols = autoclass("java.text.DateFormatSymbols")
DecimalFormat = autoclass("java.text.DecimalFormat")
DecimalFormatSymbols = autoclass("java.text.DecimalFormatSymbols")
FieldPosition = autoclass("java.text.FieldPosition")
Format = autoclass("java.text.Format")
MessageFormat = autoclass("java.text.MessageFormat")
Normalizer = autoclass("java.text.Normalizer")
NumberFormat = autoclass("java.text.NumberFormat")
ParseException = autoclass("java.text.ParseException")
ParsePosition = autoclass("java.text.ParsePosition")
RuleBasedCollator = autoclass("java.text.RuleBasedCollator")
SimpleDateFormat = autoclass("java.text.SimpleDateFormat")
StringCharacterIterator = autoclass("java.text.StringCharacterIterator")

AbstractCollection = autoclass("java.util.AbstractCollection")
AbstractList = autoclass("java.util.AbstractList")
AbstractMap = autoclass("java.util.AbstractMap")
AbstractQueue = autoclass("java.util.AbstractQueue")
AbstractSequentialList = autoclass("java.util.AbstractSequentialList")
AbstractSet = autoclass("java.util.AbstractSet")
ArrayDeque = autoclass("java.util.ArrayDeque")
ArrayList = autoclass("java.util.ArrayList")
Arrays = autoclass("java.util.Arrays")
Base64 = autoclass("java.util.Base64")
BitSet = autoclass("java.util.BitSet")
Calendar = autoclass("java.util.Calendar")
Collection = autoclass("java.util.Collection")
Collections = autoclass("java.util.Collections")
Comparator = autoclass("java.util.Comparator")
ConcurrentModificationException = autoclass(
    "java.util.ConcurrentModificationException"
)
Currency = autoclass("java.util.Currency")
Date = autoclass("java.util.Date")
Deque = autoclass("java.util.Deque")
Dictionary = autoclass("java.util.Dictionary")
DoubleSummaryStatistics = autoclass("java.util.DoubleSummaryStatistics")
DuplicateFormatFlagsException = autoclass(
    "java.util.DuplicateFormatFlagsException"
)
EmptyStackException = autoclass("java.util.EmptyStackException")
Enumeration = autoclass("java.util.Enumeration")
EventListener = autoclass("java.util.EventListener")
EventListenerProxy = autoclass("java.util.EventListenerProxy")
EventObject = autoclass("java.util.EventObject")
FormatFlagsConversionMismatchException = autoclass(
    "java.util.FormatFlagsConversionMismatchException"
)
Formattable = autoclass("java.util.Formattable")
FormattableFlags = autoclass("java.util.FormattableFlags")
Formatter = autoclass("java.util.Formatter")
FormatterClosedException = autoclass("java.util.FormatterClosedException")
GregorianCalendar = autoclass("java.util.GregorianCalendar")
HashMap = autoclass("java.util.HashMap")
HashSet = autoclass("java.util.HashSet")
Hashtable = autoclass("java.util.Hashtable")
IdentityHashMap = autoclass("java.util.IdentityHashMap")
IllegalFormatCodePointException = autoclass(
    "java.util.IllegalFormatCodePointException"
)
IllegalFormatConversionException = autoclass(
    "java.util.IllegalFormatConversionException"
)
IllegalFormatException = autoclass("java.util.IllegalFormatException")
IllegalFormatFlagsException = autoclass(
    "java.util.IllegalFormatFlagsException"
)
IllegalFormatPrecisionException = autoclass(
    "java.util.IllegalFormatPrecisionException"
)
IllegalFormatWidthException = autoclass(
    "java.util.IllegalFormatWidthException"
)
IllformedLocaleException = autoclass("java.util.IllformedLocaleException")
InputMismatchException = autoclass("java.util.InputMismatchException")
IntSummaryStatistics = autoclass("java.util.IntSummaryStatistics")
Iterator = autoclass("java.util.Iterator")
LinkedHashMap = autoclass("java.util.LinkedHashMap")
LinkedHashSet = autoclass("java.util.LinkedHashSet")
LinkedList = autoclass("java.util.LinkedList")
List = autoclass("java.util.List")
ListIterator = autoclass("java.util.ListIterator")
Locale = autoclass("java.util.Locale")
LongSummaryStatistics = autoclass("java.util.LongSummaryStatistics")
Map = autoclass("java.util.Map")
MissingFormatArgumentException = autoclass(
    "java.util.MissingFormatArgumentException"
)
MissingFormatWidthException = autoclass(
    "java.util.MissingFormatWidthException"
)
MissingResourceException = autoclass("java.util.MissingResourceException")
NavigableMap = autoclass("java.util.NavigableMap")
NavigableSet = autoclass("java.util.NavigableSet")
NoSuchElementException = autoclass("java.util.NoSuchElementException")
Objects = autoclass("java.util.Objects")
Observable = autoclass("java.util.Observable")
Observer = autoclass("java.util.Observer")
Optional = autoclass("java.util.Optional")
OptionalDouble = autoclass("java.util.OptionalDouble")
OptionalInt = autoclass("java.util.OptionalInt")
OptionalLong = autoclass("java.util.OptionalLong")
PrimitiveIterator = autoclass("java.util.PrimitiveIterator")
PriorityQueue = autoclass("java.util.PriorityQueue")
Queue = autoclass("java.util.Queue")
Random = autoclass("java.util.Random")
RandomAccess = autoclass("java.util.RandomAccess")
Set = autoclass("java.util.Set")
SimpleTimeZone = autoclass("java.util.SimpleTimeZone")
SortedMap = autoclass("java.util.SortedMap")
SortedSet = autoclass("java.util.SortedSet")
Spliterator = autoclass("java.util.Spliterator")
Spliterators = autoclass("java.util.Spliterators")
Stack = autoclass("java.util.Stack")
StringJoiner = autoclass("java.util.StringJoiner")
StringTokenizer = autoclass("java.util.StringTokenizer")
TimeZone = autoclass("java.util.TimeZone")
TooManyListenersException = autoclass("java.util.TooManyListenersException")
TreeMap = autoclass("java.util.TreeMap")
TreeSet = autoclass("java.util.TreeSet")
UUID = autoclass("java.util.UUID")
UnknownFormatConversionException = autoclass(
    "java.util.UnknownFormatConversionException"
)
UnknownFormatFlagsException = autoclass(
    "java.util.UnknownFormatFlagsException"
)
Vector = autoclass("java.util.Vector")
