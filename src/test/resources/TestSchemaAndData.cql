CREATE TABLE users(
    firstname text,
    lastname text,
    age int,
    email text,
    city text,
    PRIMARY KEY(email)
);

CREATE TABLE verified_users(
    firstname text,
    lastname text,
    age int,
    email text,
    city text,
    PRIMARY KEY(email)
);

INSERT INTO users (firstname, lastname, age, email, city) VALUES ('John', 'Smith', 46, 'johnsmith@email.com', 'Sacramento');
INSERT INTO users (firstname, lastname, age, email, city) VALUES ('Jane', 'Doe', 36, 'janedoe@email.com', 'Beverly Hills');
INSERT INTO users (firstname, lastname, age, email, city) VALUES ('Rob', 'Byrne', 24, 'robbyrne@email.com', 'San Diego');

