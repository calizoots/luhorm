CREATE TABLE IF NOT EXISTS entry (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    userid            INTEGER NOT NULL,
    location          TEXT NOT NULL,
    "coverLocation"     TEXT NOT NULL,
    title             TEXT NOT NULL,
    artist            TEXT NOT NULL,
    album             TEXT,
    year              INTEGER,
    "dateAdded"         INTEGER NOT NULL,
    size              INTEGER NOT NULL,
    FOREIGN KEY(userid) REFERENCES users(id)
);

DROP INDEX IF EXISTS idx_entry_identity;
CREATE UNIQUE INDEX idx_entry_identity ON entry(userid, location);
