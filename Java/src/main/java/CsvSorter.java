import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.*;

public class CsvSorter {

    public static void main(String[] args) throws IOException {
        // Load configuration
//        Properties config = loadConfig("resources/config.properties");
        Properties config = new Properties();
        try (InputStream input = CsvSorter.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                System.out.println("Sorry, config.properties not found in classpath.");
                return;
            }
            config.load(input);
        } catch (IOException e) {
            System.out.println("Ooops, Failed to load configuration.\n" + e.getLocalizedMessage());
        }

        // Configuration values
        String inputFilePath = config.getProperty("input.file.path");
        String outputFilePath = config.getProperty("output.file.path");
        int maxRecordsInMemory = Integer.parseInt(config.getProperty("max.records.in.memory"));
        int keyFieldIndex = Integer.parseInt(config.getProperty("key.field.index"));

        // Read the input file, split it into manageable chunks, sort, and save each chunk
        try {
            List<File> sortedFileChunks = readSplitAndSort(inputFilePath, maxRecordsInMemory, keyFieldIndex);
            mergeSortedChunks(sortedFileChunks, keyFieldIndex, outputFilePath);
        } catch (IOException e) {
            System.out.println("Failed to sort file: " + e.getMessage());
        }
    }

    private static List<File> readSplitAndSort(String inputFilePath, int maxRecordsInMemory, int keyFieldIndex) throws IOException {
        File inputFile = new File(inputFilePath);
        List<File> sortedFileChunks = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build());
            Map<String, Integer> headerMap = csvParser.getHeaderMap();
            List<String> headers = new ArrayList<>(headerMap.keySet());
            List<CSVRecord> chunk = new ArrayList<>();

            for (CSVRecord record : csvParser) {
                chunk.add(record);
                if (chunk.size() >= maxRecordsInMemory) {
                    sortedFileChunks.add(sortAndWriteToTempFile(chunk, headers, keyFieldIndex));
                    chunk.clear();
                }
            }

            if (!chunk.isEmpty()) {
                sortedFileChunks.add(sortAndWriteToTempFile(chunk, headers, keyFieldIndex));
            }
        }
        return sortedFileChunks;
    }

    // Provided utility for writing sorted data to a temporary file
    private static File sortAndWriteToTempFile(List<CSVRecord> chunk, List<String> headers, int keyFieldIndex) throws IOException {
        chunk.sort(Comparator.comparing(r -> r.get(keyFieldIndex)));
        File tempFile = File.createTempFile("sorted_chunk_", ".csv");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.builder().setHeader(headers.toArray(new String[0])).build());
            for (CSVRecord record : chunk) {
                printer.printRecord(record);
            }
        }
        return tempFile;
    }

    // TODO: Implement the merge logic for combining sorted chunks
    private static void mergeSortedChunks(List<File> sortedFileChunks, int keyFieldIndex, String outputFilePath) throws IOException {
        List<CSVParser> parsers = new ArrayList<>();
        PriorityQueue<CSVRow> pq = new PriorityQueue<>();

        List<String> headers = null;

        /* pq.size() ≤ number of input chunk files (sortedFileChunks.size())
        1. first we put K elements into the minHeap (as the number of files)
        2. then we poll 1
        3. then we put 1
        4. overall - there are K elements at any given time in the minHeap

        nothing has been changed in the code I wrote.
        */


        for (File file : sortedFileChunks) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build());
            parsers.add(csvParser);

            Iterator<CSVRecord> iter = csvParser.iterator();
            if (iter.hasNext()) {
                CSVRecord record = iter.next();
                if (headers == null) {
                    headers = new ArrayList<>(record.getParser().getHeaderMap().keySet());
                }
                pq.add(new CSVRow(record, iter, keyFieldIndex));
            }
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath));
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.builder().setHeader(headers.toArray(new String[0])).build())) {
            while (!pq.isEmpty()) {
                CSVRow smallest = pq.poll();
                printer.printRecord(smallest.record);
                CSVRow next = smallest.nextRow();
                if (next != null) {
                    pq.add(next);
                }
            }
        }

        finally {
            for (CSVParser parser : parsers) {
                parser.close();
            }
            for (File file: sortedFileChunks) {
                file.delete();
            }
        }
    }

    static class CSVRow implements Comparable<CSVRow> {
        CSVRecord record;
        Iterator<CSVRecord> iterator;
        int sortByIndex;

        public CSVRow(CSVRecord record, Iterator<CSVRecord> iterator, int sortByIndex) {
            this.record = record;
            this.iterator = iterator;
            this.sortByIndex = sortByIndex;
        }

        @Override
        public int compareTo(CSVRow other) {
            return this.record.get(sortByIndex).compareTo(other.record.get(sortByIndex));
        }

        public CSVRow nextRow() {
            return iterator.hasNext() ? new CSVRow(iterator.next(), iterator, sortByIndex) : null;
        }
    }

    private static Properties loadConfig(String configFilePath) throws IOException {
        Properties config = new Properties();
        try (InputStream input = new FileInputStream(configFilePath)) {
            config.load(input);
        }
        return config;
    }



}