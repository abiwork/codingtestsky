package uk.sky;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;


/**
 * Filters log file reader. Expects input to be in a fixed comma separated format following the pattern. Expects a fixed header line entry which is skipped at the start. This is done for
 * simplicity and performance to avoid checking for header line entry on each read line.
 * <p>
 * Actions.
 * 1. Skips the header
 * 2. Builds a list of {@link LogExtractLine}
 * 3. Applies the condition and returns a collection of {@link LogExtractLine}
 * <p>
 * Things to note.
 * 1. The input reader is consumed after each method call and closed. It cannot be re-used for reading again. It is one time read operation only.
 * 2. Assuming the size of the reader is within the JVM limits and no {@link OutOfMemoryError} occurs while building log lines and returning a collection. If this assumption fails then a
 * limit based read needs to be adopted. This is out of scope
 * 3. Response times are within the range for {@link Long}
 * 4. Any I/O failures during read will rethrow the exception as {@link IllegalStateException}
 */
public class DataFilterer
{
	private static final String HEADER_LINE = "REQUEST_TIMESTAMP,COUNTRY_CODE,RESPONSE_TIME" + System.lineSeparator();
	private static final Logger logger = Logger.getGlobal();

	public static Collection<?> filterByCountry(final Reader source, final String country)
	{
		skipHeader(source);
		final List<LogExtractLine> logLines = buildLogLines(source);

		return logLines.stream().filter(logLine -> logLine.country.equals(country)).collect(Collectors.toList());
	}

	public static Collection<?> filterByCountryWithResponseTimeAboveLimit(final Reader source, final String country, final long limit)
	{
		skipHeader(source);
		final List<LogExtractLine> logLines = buildLogLines(source);

		return logLines.stream().filter(logLine -> logLine.country.equals(country) && logLine.responseTime > limit)
				.collect(Collectors.toList());
	}

	public static Collection<?> filterByResponseTimeAboveAverage(final Reader source)
	{
		skipHeader(source);
		final List<LogExtractLine> logLines = buildLogLines(source);

		final Double averageResponseTime = logLines.stream().collect(Collectors.averagingLong(line -> line.responseTime));
		return logLines.stream().filter(line -> line.responseTime > averageResponseTime).collect(Collectors.toList());
	}

	/**
	 * builds a list of log lines reading the source. Throws {@link IllegalStateException} if any IO exceptions occur during read
	 *
	 * @param source source to read
	 * @return list of log lines
	 */
	private static List<LogExtractLine> buildLogLines(final Reader source)
	{
		String lineRead;
		final List<LogExtractLine> logLines = new ArrayList<>();
		try (final BufferedReader bufferedLogReader = new BufferedReader(source))
		{
			while ((lineRead = bufferedLogReader.readLine()) != null)
			{
				final LogExtractLine logExtractLine = createLogExtractLine(lineRead);
				if (logExtractLine != null)
					logLines.add(logExtractLine);
			}
		}
		catch (final IOException e)
		{
			throw new IllegalStateException("Failed while reading log file ", e);
		}

		return logLines;
	}

	private static LogExtractLine createLogExtractLine(final String commaSeparatedLogLine)
	{
		final String[] logLineItems = commaSeparatedLogLine.split(",");

		try
		{
			return new LogExtractLine(logLineItems);
		}
		catch (final IllegalArgumentException e)
		{
			logger.log(Level.WARNING, "Line failed parsing. Ignoring line \n" + commaSeparatedLogLine, e);
		}

		return null;
	}

	private static void skipHeader(final Reader source)
	{
		final int skipLength = HEADER_LINE.length();
		boolean skipped = false;
		try
		{
			final long numberOfCharactersSkipped = source.skip(skipLength);
			skipped = numberOfCharactersSkipped == skipLength;
		}
		catch (final IOException e1)
		{
			throw new IllegalStateException("Unable to skip header line", e1);
		}

		if (!skipped)
			throw new IllegalStateException("Unexpected format in header line. Unable to skip required characters");
	}

	/**
	 * an immutable class representing the log extract line. Only two fields (country and response time) are captured at the moment which were used by the implementation.
	 */
	private static final class LogExtractLine
	{
		private final String country;
		private final long responseTime;

		private LogExtractLine(final String... logLineItems) throws NumberFormatException
		{
			if (logLineItems.length != 3)
				throw new IllegalArgumentException("Expected three items for lone line but received " + logLineItems);

			this.country = logLineItems[1];
			this.responseTime = Long.valueOf(logLineItems[2]);
		}
	}
}
