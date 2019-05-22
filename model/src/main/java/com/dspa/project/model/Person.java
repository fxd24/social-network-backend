package com.dspa.project.model;

// id(Long) | firstName(String) | lastName(String) | gender(String) | birthday(Date) | creationDate(DateTime) | locationIP(String) | browserUsed(String)

import java.util.Date;
import java.util.Objects;

public class Person {

    private final long id;
    private final String firstName;
    private final String lastName;
    private final String gender;
    private final Date birthday;
    private final Date creationDate; // should be datetime
    private final String locationIP;
    private final String browserUsed;


    private Person(final Builder builder) {

        this.id = builder.id;
        this.firstName = builder.firstName;
        this.lastName = builder.lastName;
        this.gender = builder.gender;
        this.birthday = builder.birthday;
        this.creationDate = builder.creationDate;
        this.browserUsed = builder.browserUsed;
        this.locationIP = builder.locationIP;


    }

    // GETTERS


    public long getId() {
        return id;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getGender() {
        return gender;
    }

    public Date getBirthday() {
        return birthday;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public String getLocationIP() {
        return locationIP;
    }

    public String getBrowserUsed() {
        return browserUsed;
    }

    // OTHER STANDARD METHODS

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person that = (Person) o;
        return id == that.id &&
                firstName.equals(that.firstName) &&
                lastName.equals(that.lastName) &&
                Objects.equals(gender, that.gender) &&
                Objects.equals(birthday, that.birthday) &&
                Objects.equals(creationDate, that.creationDate) &&
                Objects.equals(locationIP, that.locationIP) &&
                Objects.equals(browserUsed, that.browserUsed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed);
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", gender='" + gender + '\'' +
                ", birthday=" + birthday +
                ", creationDate=" + creationDate +
                ", locationIP='" + locationIP + '\'' +
                ", browserUsed='" + browserUsed + '\'' +
                '}';
    }

    // BUILDER

    public static class Builder {

        private long id;
        private String firstName;
        private String lastName;
        private String gender;
        private Date birthday;
        private Date creationDate;
        private String browserUsed;
        private String locationIP;


        public Builder id(final long id) {
            this.id = id;
            return this;
        }

        public Builder firstName(final String firstName) {
            this.firstName = firstName;
            return this;
        }

        public Builder lastName(final String lastName) {
            this.lastName = lastName;
            return this;
        }

        public Builder gender(final String gender) {
            this.gender = gender;
            return this;
        }

        public Builder birthday(final Date birthday) {
            this.birthday = birthday;
            return this;
        }

        public Builder creationDate(final Date creationDate) {
            this.creationDate = creationDate;
            return this;
        }


        public Builder browserUsed(final String browserUsed) {
            this.browserUsed = browserUsed;
            return this;
        }

        public Builder locationIP(final String locationIP) {
            this.locationIP = locationIP;
            return this;
        }

        public Person build() {
            return new Person(this);
        }
    }


}
