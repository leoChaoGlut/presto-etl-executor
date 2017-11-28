CREATE TABLE placeholder_record
(
  id           VARCHAR(256) NOT NULL
  COMMENT 'value format: schema.table'
    PRIMARY KEY,
  placeholders TEXT         NULL
);
