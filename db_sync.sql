CREATE SCHEMA humancount
    AUTHORIZATION lrwrt;

GRANT ALL ON SCHEMA humancount TO lrwrt;

ALTER DEFAULT PRIVILEGES FOR ROLE lrwrt IN SCHEMA humancount
GRANT ALL ON TABLES TO lrwrt;

CREATE TABLE humancount.rawsensordata
(
    recdt timestamp without time zone DEFAULT now(),
    sensor bigint,
    value bigint
);

ALTER TABLE IF EXISTS humancount.rawsensordata
    OWNER to lrwrt;

GRANT ALL ON TABLE humancount.rawsensordata TO lrwrt;