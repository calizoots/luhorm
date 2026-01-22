CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username  TEXT NOT NULL UNIQUE,
    password  TEXT NOT NULL,
    dateAdded INTEGER NOT NULL
);

INSERT INTO users (username, password, dateAdded) VALUES (
       'bine',
       '$argon2id$v=19$m=19456,t=2,p=1$pgqyEXO1E80Qzl11hvkjBw$B8zycIwBtSAHGlxJtDNl2Pcq50bGxO8swRiKCHTaCVQ',
       strftime('%s','now')
);

INSERT INTO users (username, password, dateAdded) VALUES (
       'bine2',
       '$argon2id$v=19$m=19456,t=2,p=1$pgqyEXO1E80Qzl11hvkjBw$B8zycIwBtSAHGlxJtDNl2Pcq50bGxO8swRiKCHTaCVQ',
       strftime('%s','now')
);
