package shpp.azaika.util;

import shpp.azaika.pojo.UserPojo;

import java.io.IOException;

public interface Writer {
    void write(UserPojo userPojo) throws IOException;
}
