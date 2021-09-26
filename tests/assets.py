import datetime

START_DATE = datetime.datetime(2020, 1, 1, 0, 0, 0)
END_DATE = datetime.datetime(2020, 1, 1, 4, 0, 0)

source_ddl_transactions = """
CREATE TABLE transactions (
  id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  idoper TINYINT(4) NOT NULL,
  move TINYINT(4) NOT NULL,
  amount DECIMAL(10, 2) NOT NULL,
  PRIMARY KEY (id)
)
ENGINE = INNODB
"""

source_ddl_type_opers = """
CREATE TABLE operation_types (
  id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(50) NOT NULL,
  PRIMARY KEY (id)
)
ENGINE = INNODB
"""


source_data_opers = [
    (1, 'Subscription purchase'),
    (2, 'Subscription update'),
    (3, 'Account deposit')
]


source_data_transactions = [
    (datetime.datetime(2020, 1, 1, 0, 0, 0), 1, -1, 100),
    (datetime.datetime(2020, 1, 1, 1, 0, 0), 1, -1, 100),

    (datetime.datetime(2020, 1, 1, 2, 0, 0), 2, -1, 100),
    (datetime.datetime(2020, 1, 1, 2, 0, 0), 2, -1, 100),

    (datetime.datetime(2020, 1, 1, 3, 0, 0), 3, 1, 150),
    (datetime.datetime(2020, 1, 1, 3, 0, 0), 3, 1, 200)
]


destination_ddl_transactions = """
CREATE TABLE transactions_denormalized (
  id INT(11) UNSIGNED NOT NULL,
  dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  idoper TINYINT(4) NOT NULL,
  move TINYINT(4) NOT NULL,
  amount DECIMAL(10, 2) NOT NULL,
  name_oper VARCHAR(50) NOT NULL,
  PRIMARY KEY (id)
)
ENGINE = INNODB

"""

source_copy_data = f"""
SELECT
    t.id,
    t.dt,
    t.idoper,
    t.move,
    t.amount,
    ot.name as name_oper
FROM transactions t
    JOIN operation_types ot ON t.idoper = ot.id
WHERE dt BETWEEN '{START_DATE}' AND '{END_DATE}'
INTO OUTFILE '/var/lib/mysql-files/transaction_denormalized.csv'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';
"""

destination_load_data = """
LOAD DATA INFILE '/var/lib/mysql-files/transaction_denormalized.csv'
INTO TABLE transactions_denormalized
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';
"""



