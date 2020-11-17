package uk.sky;

import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collection;
import org.hamcrest.core.Is;
import org.junit.Test;


public class DataFiltererTest
{
	@Test(expected = IllegalStateException.class)
	public void throwIllegalStateExceptionWhenEmptyFileWithoutHeaderIsReceived() throws FileNotFoundException
	{
		DataFilterer.filterByCountry(openFile("src/test/resources/empty"), "GB");
	}

	@Test
	public void testSingleLine() throws FileNotFoundException
	{
		final FileReader singleLineReader = openFile("src/test/resources/single-line");
		final Collection<?> filterByCountryGB = DataFilterer.filterByCountry(singleLineReader, "GB");
		assertThat(filterByCountryGB.size(), Is.is(1));
	}

	@Test
	public void testMultiLines() throws FileNotFoundException
	{
		final String testFilePath = "src/test/resources/multi-lines";
		assertThat(DataFilterer.filterByCountry(openFile(testFilePath), "US").size(), Is.is(3));
		assertThat(DataFilterer.filterByCountry(openFile(testFilePath), "GB").size(), Is.is(1));
		assertThat(DataFilterer.filterByCountry(openFile(testFilePath), "DE").size(), Is.is(1));
	}

	@Test
	public void testMultiLinesAboveLimit() throws FileNotFoundException
	{
		final String testFilePath = "src/test/resources/multi-lines";
		assertThat(DataFilterer.filterByCountryWithResponseTimeAboveLimit(openFile(testFilePath), "US", 800).size(), Is.is(1));
		assertThat(DataFilterer.filterByCountryWithResponseTimeAboveLimit(openFile(testFilePath), "GB", 30).size(), Is.is(1));
		assertThat(DataFilterer.filterByCountryWithResponseTimeAboveLimit(openFile(testFilePath), "DE", 500).size(), Is.is(0));
	}

	@Test
	public void testMultiLinesAboveAverageResponseTime() throws FileNotFoundException
	{
		final String testFilePath = "src/test/resources/multi-lines";
		assertThat(DataFilterer.filterByResponseTimeAboveAverage(openFile(testFilePath)).size(), Is.is(3));
	}

	private FileReader openFile(final String filename) throws FileNotFoundException
	{
		return new FileReader(new File(filename));
	}
}
