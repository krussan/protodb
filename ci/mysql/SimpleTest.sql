CREATE TABLE `SimpleTest` (
  `ID` int(11) NOT NULL,
  `_by_ID` int(11) default NULL,
  `dd` double default NULL,
  `ff` float default NULL,
  `is` int(11) default NULL,
  `il` bigint(20) default NULL,
  `bb` tinyint(1) default NULL,
  `ss` text,
  PRIMARY KEY  (`ID`)
)