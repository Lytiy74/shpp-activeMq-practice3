package shpp.azaika.pojo;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.hibernate.validator.constraints.Length;
import shpp.azaika.validation.constraints.CheckEddr;

import java.time.LocalDate;

@JsonPropertyOrder({ "name", "eddr", "count", "date" })
public class UserPojo {
    @NotNull
    @Length(min = 7)
    @Pattern(regexp = "(?iu).*[aа].*", message = "Name must contains at least one letter 'A'")
    private String name;
    @NotNull
    @CheckEddr(message = "Invalid eddr number")
    private String eddr;
    @Min(value = 10, message = "Count must be >= 10")
    private int count;
    @NotNull
    private LocalDate date;

    public UserPojo() {
    }

    public UserPojo(String name, String eddr, int count, LocalDate date) {
        this.name = name;
        this.eddr = eddr;
        this.count = count;
        this.date = date;
    }

    public String getName() {
        return name;
    }

    public String getEddr() {
        return eddr;
    }

    public int getCount() {
        return count;
    }

    public LocalDate getDate() {
        return date;
    }

    @Override
    public String toString() {
        return "UserPojo{" + "name='" + name + '\'' +
                ", eddr='" + eddr + '\'' +
                ", count=" + count +
                ", date=" + date +
                '}';
    }
}
