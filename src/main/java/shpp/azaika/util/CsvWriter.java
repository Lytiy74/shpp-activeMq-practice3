package shpp.azaika.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shpp.azaika.pojo.UserPojo;

import java.io.*;


public class CsvWriter implements Writer {
    private static final Logger logger = LoggerFactory.getLogger(CsvWriter.class);
    private final CsvMapper csvMapper;
    private final FileOutputStream outputStream;
    public CsvWriter(String fileName) throws FileNotFoundException {
        this.outputStream = new FileOutputStream(fileName,true);
        this.csvMapper = (CsvMapper) new CsvMapper().registerModule(new JavaTimeModule());
        this.csvMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        logger.debug("CsvWriter initialized");
    }

    @Override
    public void write(UserPojo userPojo) throws IOException {
        CsvSchema schema = csvMapper.schemaFor(UserPojo.class);
        csvMapper.writer(schema).writeValue(outputStream,userPojo);
    }
}
