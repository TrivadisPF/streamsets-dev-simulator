# Dev Simulator Origin

Supported pipeline types: **Data Collector**

The Dev Simulator Origin ...

## File Name Pattern and Mode

Use a file name pattern to define the files that the Directory origin processes. You can use either a glob pattern or a regular expression to define the file name pattern.

The Directory origin processes files based on the file name pattern mode, file name pattern, and specified directory. For example, if you specify a `/logs/weblog/` directory, glob mode, and *.json as the file name pattern, the origin processes all files with the json extension in the /logs/weblog/ directory.

The origin processes files in order based on the specified read order.

For more information about glob syntax, see the Oracle Java documentation. For more information about regular expressions, see Regular Expressions Overview.

## Reading from Subdirectories

## Record Header Attributes

The Dev Simulator origin creates record header attributes that include information about the originating file for the record.

You can use the `record:attribute` or `record:attributeOrDefault` functions to access the information in the attributes. For more information about working with record header attributes, see Working with Header Attributes.

The Directory origin creates the following record header attributes:

  * baseDir - Base directory containing the file where the record originated.
  * filename - Provides the name of the file where the record originated.
  * file - Provides the file path and file name where the record originated.

## Event Generation

The Dev Simulator origin can generate events that you can use in an event stream. When you enable event generation, the origin generates event records each time the origin starts or completes reading a file. It can also generate events when it completes processing all available data and the configured batch wait time has elapsed.

### Event Records

t.b.d.

## Buffer Limit and Error Handling

The Directory origin passes each record to a buffer. The size of the buffer determines the maximum size of the record that can be processed. Decrease the buffer limit when memory on the Data Collector machine is limited. Increase the buffer limit to process larger records when memory is available.

When a record is larger than the specified limit, the Directory origin processes the source file based on the stage error handling:

**Discard**

The origin discards the record and all remaining records in the file, and then continues processing the next file.

**Send to Error**

With a buffer limit error, the origin cannot send the record to the pipeline for error handling because it is unable to fully process the record.
Instead, the origin creates a message stating that a buffer overrun error occurred. The message includes the file and offset where the buffer overrun error occurred. The information displays in the pipeline history and displays as an alert when you monitor the pipeline.

If an error directory is configured for the stage, the origin moves the file to the error directory and continues processing the next file.

**Stop Pipeline** 

The origin stops the pipeline and creates a message stating that a buffer overrun error occurred. The message includes the file and offset where the buffer overrun error occurred. The information displays as an alert and in the pipeline history.

**Note**: You can also check the Data Collector log file for error details.

## Data Formats

The Directory origin processes data differently based on the data format.

The Directory origin processes data formats as follows:

**Delimited** 

Generates a record for each delimited line. You can use the following delimited format types:
  
  * **Default CSV** - File that includes comma-separated values. Ignores empty lines in the file.
  * **RFC4180 CSV** - Comma-separated file that strictly follows RFC4180 guidelines.
  * **MS Excel CSV** - Microsoft Excel comma-separated file.
  * **MySQL CSV** - MySQL comma-separated file.
  * **Tab-Separated Values** - File that includes tab-separated values.
  * **PostgreSQL CSV** - PostgreSQL comma-separated file.
  * **PostgreSQL Text** - PostgreSQL text file.
  * **Custom** - File that uses user-defined delimiter, escape, and quote characters.
  * **Multi Character Delimited** - File that uses multiple user-defined characters to delimit fields and lines, and single user-defined escape and quote characters.
 
You can use a list or list-map root field type for delimited data, and optionally include field names from a header line, when available. For more information about the root field types, see Delimited Data Root Field Type.

When using a header line, you can enable handling records with additional columns. The additional columns are named using a custom prefix and integers in sequential increasing order, such as _extra_1, _extra_2. When you disallow additional columns, records that include additional columns are sent to error.

You can also replace a string constant with null values.

When a record exceeds the maximum record length defined for the stage, the origin cannot continue reading the file. Records already read from the file are passed to the pipeline. The behavior of the origin is then based on the error handling configured for the stage:

  * **Discard** - The origin continues processing with the next file, leaving the partially-processed file in the directory.
  * **To Error** - The origin continues processing with the next file. If a post-processing error directory is configured for the stage, the origin moves the partially-processed file to the error directory. Otherwise, it leaves the file in the directory.
  * **Stop Pipeline** - The origin stops the pipeline.

**Excel**

Not yet supported!

**JSON**

Not yet supported!

## Configuring a Dev Simultor Origin

Configure a Dev Simulator origin to read data from files in a directory.

When you configure Dev Simulator, you define file properties, including the data format to process. Then, you define post-processing options and the properties specific to the data format.

1. In the Properties panel, on the **General** tab, configure the following properties

General Property | Description
------------- | -------------
Name  | Stage name.
Description  | Optional description.
Produce Events | Generates event records when event occurs. Use for [event handling](https://streamsets.com/documentation/datacollector/3.20.x/help/datacollector/UserGuide/Event_Handling/EventFramework-Title.html#concept_cph_5h4_lx). 
On Record Error  | Error record handling for the stage

2. On the **Files** tab, configure the following properties:

File Property | Description
------------- | -------------
Files Directory  | A directory local to Data Collector where source files are stored. Enter an absolute path.
Include Subdirectories  | Reads files in any subdirectory of the specified file directory. 
File Name Pattern  | Pattern of the file names to process. Use wildcard patterns or regular expressions based on the specified file name pattern mode.
Multiple Record Types | Enables handling different message types on the Multi Record Types tab
Batch Size | no used 
File Name Pattern Mode | Syntax of the file name pattern, either **Wildcard** or **Regular Expression**

3. On the **Event Time** tab, configure the following properties:

Event Time Property | Description
------------- | -------------
Timestamp Mode  | 
Timestamp Field  | 
Timestamp Format  | 
Event Timestamp Output Field | 
Simulation Start Time is Now | 
Simulation Start Timestamp | 
Date Format | Format of the date, datetime, or time data. Select the format to use or create a custom format.
Other Date Format | Use to enter a custom date format. For more information about creating a custom date format, see the [Oracle Java documentation](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html).

3. On the **Data Format** tab, configure the following properties:

Date Format Property | Description
------------- | -------------
Timestamp Mode  | 
Timestamp Field  | 
Timestamp Format  | 
Event Timestamp Output Field | 
Simulation Start Time is Now | 
Simulation Start Timestamp | 
Date Format | 
Other Date Format | 

3. On the **Multi Record Types** tab, configure the following properties:

Multi Record Types Property | Description
------------- | -------------

3. On the **Data Format** tab, configure the following properties:

Data Fomrmat Property | Description
------------- | -------------
Input Data Format | Delimiter format type. Use one of the following options. 
Delimiter Format Type | Delimiter character for a custom delimiter format. Select one of the available options or use Other to enter a custom character.
Header Line | Indicates whether a file contains a header line, and whether to use the header line.
Lines to Skip | Number of lines to skip before reading data.
Root Field Type | Root field type to use: <br/><br/>**List-Map** - Generates an indexed list of data. Enables you to use standard functions to process data. Use for new pipelines. <br/>**List** - Generates a record with an indexed list with a map for header and value. Requires the use of delimited data functions to process data. Use only to maintain pipelines created before 1.1.0.
Parse NULLs | Replaces the specified string constant with null values. 
Charset |  Character encoding of the files to be processed.
Ignore Control Characters |  Removes all ASCII control characters except for the tab, line feed, and carriage return characters.
