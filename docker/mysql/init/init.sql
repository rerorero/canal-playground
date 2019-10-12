CREATE USER canal IDENTIFIED BY 'canal';  

GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%' ;
FLUSH PRIVILEGES;

CREATE TABLE IF NOT EXISTS `test` (
  `id` INT(11) NOT NULL,
  `name` VARCHAR(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
