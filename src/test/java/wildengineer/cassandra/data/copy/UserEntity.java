package wildengineer.cassandra.data.copy;

/**
 * Created by mgroves on 6/4/16.
 */
public class UserEntity {

    private String email;

    private String lastname;
    private String firstname;
    private int age;
    private String city;

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UserEntity)) {
            return false;
        }

        UserEntity that = (UserEntity) o;

        if (age != that.age) {
            return false;
        }
        if (!email.equals(that.email)) {
            return false;
        }
        if (!lastname.equals(that.lastname)) {
            return false;
        }
        if (!firstname.equals(that.firstname)) {
            return false;
        }
        return city.equals(that.city);

    }

    @Override
    public int hashCode() {
        int result = email.hashCode();
        result = 31 * result + lastname.hashCode();
        result = 31 * result + firstname.hashCode();
        result = 31 * result + age;
        result = 31 * result + city.hashCode();
        return result;
    }
}
