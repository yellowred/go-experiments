# Test Postgres

## Setup Db

1. Create table:
    ```
    CREATE TABLE auditor_txs (
      id serial PRIMARY KEY,
      block_height integer NOT NULL,
      transaction_hash character varying(64) NOT NULL,
      identifier integer NOT NULL,
      from_wallet character varying(35),
      to_wallet character varying(35),
      value numeric,
      purpose character varying(64)
    );
    ```
1. Create index:
    ```
    CREATE UNIQUE INDEX auditor_txs_key ON auditor_txs USING btree (block_height, transaction_hash, identifier);
    ```