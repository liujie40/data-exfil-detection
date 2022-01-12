-- -------------------------------------------------- --
-- Creates test data to be used in acceptance testing --
--                                                    --
-- Author: Daniel Yates                               --
-- -------------------------------------------------- --

INSERT INTO `data-exfil-detection.test_data.netflow`
VALUES(60,99,'Device2','Device1',8,'Port3','Port1',1,0,100,0),
(60,59,'Device2','Device3',4,'Port1','Port2',2,2,200,200),
(120,10,'Device1','Device3',2,'Port2','Port1',1,1,100,100),
(120,17,'Device1','Device3',6,'Port3','Port3',1,1,100,100),
(240,64,'Device3','Device2',7,'Port1','Port1',1,0,100,0),
(300,88,'Device1','Device2',6,'Port1','Port2',0,1,0,100),
(360,72,'Device1','Device2',5,'Port1','Port3',0,1,0,100),
(360,95,'Device2','Device3',4,'Port1','Port1',1,0,50,0),
(420,87,'Device1','Device3',7,'Port2','Port3',1,1,50,50),
(420,9,'Device3','Device1',6,'Port1','Port3',1,1,100,100),
(420,77,'Device3','Device1',5,'Port2','Port3',1,0,100,0),
(480,61,'Device1','Device3',5,'Port1','Port1',2,0,200,0),
(540,33,'Device1','Device3',3,'Port1','Port2',1,0,100,0),
(600,24,'Device1','Device2',6,'Port2','Port1',0,1,0,50),
(600,79,'Device1','Device3',2,'Port2','Port1',2,1,200,20),
(660,87,'Device3','Device1',7,'Port2','Port2',1,0,30,0),
(720,66,'Device2','Device3',6,'Port2','Port1',1,0,100,0),
(780,8,'Device2','Device3',1,'Port2','Port2',2,0,200,0),
(780,2,'Device2','Device3',8,'Port2','Port3',1,0,100,0),
(840,64,'Device3','Device1',8,'Port2','Port1',0,1,0,100),
(840,31,'Device3','Device1',2,'Port2','Port3',0,1,0,100)

INSERT INTO `data-exfil-detection.test_data.device_level_data`
VALUES('Device1','1970-01-01 00:05:00','Port1',100,0),
('Device1','1970-01-01 00:05:00','Port2',100,100),
('Device1','1970-01-01 00:05:00','Port3',100,100),
('Device1','1970-01-01 00:10:00','Port1',200,300),
('Device1','1970-01-01 00:10:00','Port2',50,50),
('Device1','1970-01-01 00:10:00','Port3',200,100),
('Device1','1970-01-01 00:15:00','Port1',0,100),
('Device1','1970-01-01 00:15:00','Port2',100,200),
('Device1','1970-01-01 00:15:00','Port3',0,100),
('Device2','1970-01-01 00:05:00','Port1',300,200),
('Device2','1970-01-01 00:05:00','Port2',0,0),
('Device2','1970-01-01 00:05:00','Port3',0,100),
('Device2','1970-01-01 00:10:00','Port1',0,50),
('Device2','1970-01-01 00:10:00','Port2',0,100),
('Device2','1970-01-01 00:10:00','Port3',0,100),
('Device2','1970-01-01 00:15:00','Port1',0,50),
('Device2','1970-01-01 00:15:00','Port2',0,400),
('Device2','1970-01-01 00:15:00','Port3',0,0),
('Device3','1970-01-01 00:05:00','Port1',100,200),
('Device3','1970-01-01 00:05:00','Port2',200,200),
('Device3','1970-01-01 00:05:00','Port3',100,100),
('Device3','1970-01-01 00:10:00','Port1',350,100),
('Device3','1970-01-01 00:10:00','Port2',100,100),
('Device3','1970-01-01 00:10:00','Port3',50,50),
('Device3','1970-01-01 00:15:00','Port1',100,20),
('Device3','1970-01-01 00:15:00','Port2',400,30),
('Device3','1970-01-01 00:15:00','Port3',100,0)