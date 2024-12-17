package shpp.azaika.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.pojo.UserPojo;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class CsvWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CsvWriter.class);
    private final CsvMapper csvMapper;
    private final BufferedOutputStream outputStream;
    private final CsvSchema schema;
    private final ObjectWriter writer;

    public CsvWriter(String fileName) throws IOException {
        this.outputStream = new BufferedOutputStream(new FileOutputStream(fileName, true), 16384);
        this.csvMapper = (CsvMapper) new CsvMapper().registerModule(new JavaTimeModule());
        this.csvMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

        this.schema = csvMapper.schemaFor(UserPojo.class).withColumnSeparator(',');
        this.writer = csvMapper.writer(schema);

        logger.info("CsvWriter initialized for file: {}", fileName);
    }

    public void write(UserPojo userPojo) throws IOException {
        writer.writeValue(outputStream, userPojo);
    }

    @Override
    public void close() throws IOException {
        try {
            outputStream.flush();
        } finally {
            outputStream.close();
            logger.info("CsvWriter closed");
        }
    }
}
