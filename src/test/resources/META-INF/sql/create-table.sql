# create db user.

CREATE USER 'mykidong'@'%' IDENTIFIED BY 'icarus';
GRANT ALL PRIVILEGES ON *.* TO 'mykidong'@'%' WITH GRANT OPTION;
flush privileges;

CREATE USER 'mykidong'@'localhost' IDENTIFIED BY 'icarus';
GRANT ALL PRIVILEGES ON *.* TO 'mykidong'@'localhost' WITH GRANT OPTION;
flush privileges;

CREATE USER 'mykidong'@'amaster.example.com' IDENTIFIED BY 'icarus';
GRANT ALL PRIVILEGES ON *.* TO 'mykidong'@'amaster.example.com' WITH GRANT OPTION;
flush privileges;


# drop database.
DROP DATABASE kafkatx;

# create db and user.
CREATE DATABASE kafkatx;

use kafkatx;

CREATE TABLE `offset` (
	`topic` VARCHAR(255),
	`partition` INT,
	`offset` BIGINT,
	PRIMARY KEY (`topic`, `partition`)
);

create table `record` (
  `rid` int(11) NOT NULL AUTO_INCREMENT,
  `customer_id` VARCHAR(50) NOT NULL,
  `record` VARCHAR(400) NOT NULL,
  `when` bigint NOT NULL,
  PRIMARY KEY (`rid`)
);