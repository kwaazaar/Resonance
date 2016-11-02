CREATE DATABASE  IF NOT EXISTS `resonancedb` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `resonancedb`;
-- MySQL dump 10.13  Distrib 5.7.12, for Win64 (x86_64)
--
-- Host: localhost    Database: resonancedb
-- ------------------------------------------------------
-- Server version	5.7.14-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `consumedsubscriptionevent`
--

DROP TABLE IF EXISTS `consumedsubscriptionevent`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `consumedsubscriptionevent` (
  `Id` bigint(20) unsigned NOT NULL,
  `SubscriptionId` bigint(20) unsigned NOT NULL,
  `EventName` varchar(50) CHARACTER SET utf8mb4 DEFAULT NULL,
  `PublicationDateUtc` datetime(6) NOT NULL,
  `FunctionalKey` varchar(50) CHARACTER SET utf8mb4 NOT NULL,
  `Priority` int(11) NOT NULL,
  `PayloadId` bigint(20) unsigned DEFAULT NULL,
  `DeliveryDateUtc` datetime(6) NOT NULL,
  `ConsumedDateUtc` datetime(6) NOT NULL,
  PRIMARY KEY (`SubscriptionId`,`PublicationDateUtc`,`DeliveryDateUtc`,`Id`),
  KEY `FK_ConsumedSubscriptionEvent_EventPayload` (`PayloadId`),
  CONSTRAINT `FK_ConsumedSubscriptionEvent_EventPayload` FOREIGN KEY (`PayloadId`) REFERENCES `eventpayload` (`Id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `eventpayload`
--

DROP TABLE IF EXISTS `eventpayload`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `eventpayload` (
  `Id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Payload` longtext NOT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `failedsubscriptionevent`
--

DROP TABLE IF EXISTS `failedsubscriptionevent`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `failedsubscriptionevent` (
  `Id` bigint(20) unsigned NOT NULL,
  `SubscriptionId` bigint(20) unsigned NOT NULL,
  `EventName` varchar(50) CHARACTER SET utf8mb4 DEFAULT NULL,
  `PublicationDateUtc` datetime(6) NOT NULL,
  `FunctionalKey` varchar(50) CHARACTER SET utf8mb4 NOT NULL,
  `Priority` int(11) NOT NULL,
  `PayloadId` bigint(20) unsigned DEFAULT NULL,
  `DeliveryDateUtc` datetime(6) DEFAULT NULL,
  `FailedDateUtc` datetime(6) NOT NULL,
  `Reason` int(11) NOT NULL COMMENT '0=Unknown, 1=Expired, 2=MaxRetriesReached, 3=Other',
  `ReasonOther` varchar(1000) CHARACTER SET utf8mb4 DEFAULT NULL,
  PRIMARY KEY (`SubscriptionId`,`PublicationDateUtc`,`FailedDateUtc`,`Reason`,`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `lastconsumedsubscriptionevent`
--

DROP TABLE IF EXISTS `lastconsumedsubscriptionevent`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `lastconsumedsubscriptionevent` (
  `SubscriptionId` bigint(20) unsigned NOT NULL,
  `FunctionalKey` varchar(50) CHARACTER SET utf8mb4 NOT NULL,
  `PublicationDateUtc` datetime(6) NOT NULL,
  PRIMARY KEY (`SubscriptionId`,`FunctionalKey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `subscription`
--

DROP TABLE IF EXISTS `subscription`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `subscription` (
  `Id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Name` varchar(50) CHARACTER SET utf8mb4 NOT NULL,
  `Ordered` tinyint(1) NOT NULL,
  `TimeToLive` int(11) DEFAULT NULL COMMENT 'Time to live in seconds',
  `MaxDeliveries` int(11) NOT NULL,
  `DeliveryDelay` int(11) DEFAULT NULL COMMENT 'Delay the delivery by number of seconds',
  PRIMARY KEY (`Id`,`MaxDeliveries`),
  UNIQUE KEY `UK_Subscription_Name` (`Name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `subscriptionevent`
--

DROP TABLE IF EXISTS `subscriptionevent`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `subscriptionevent` (
  `Id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `SubscriptionId` bigint(20) unsigned NOT NULL,
  `TopicEventId` bigint(20) unsigned NOT NULL,
  `EventName` varchar(50) CHARACTER SET utf8mb4 DEFAULT NULL,
  `PublicationDateUtc` datetime(6) NOT NULL,
  `FunctionalKey` varchar(50) CHARACTER SET utf8mb4 NOT NULL,
  `Priority` int(11) NOT NULL,
  `PayloadId` bigint(20) unsigned DEFAULT NULL COMMENT 'Custom payload for this subscriber, in case the payload was transformed/modified for this subscription.',
  `ExpirationDateUtc` datetime(6) NOT NULL,
  `DeliveryDelayedUntilUtc` datetime(6) NOT NULL,
  `DeliveryCount` int(11) NOT NULL,
  `DeliveryDateUtc` datetime(6) DEFAULT NULL,
  `DeliveryKey` varchar(36) NOT NULL,
  `InvisibleUntilUtc` datetime(6) NOT NULL,
  PRIMARY KEY (`SubscriptionId`,`Priority`,`PublicationDateUtc`,`FunctionalKey`,`Id`,`ExpirationDateUtc`,`DeliveryDelayedUntilUtc`,`DeliveryCount`,`DeliveryKey`,`InvisibleUntilUtc`),
  UNIQUE KEY `UK_Id` (`Id`),
  KEY `FK_SubscriptionEvent_EventPayload` (`PayloadId`),
  KEY `FK_SubscriptionEvent_TopicEvent` (`TopicEventId`),
  KEY `FK_SubscriptionEvent_Subscription` (`SubscriptionId`),
  KEY `IX_SubscriptionEvent_Invisible` (`SubscriptionId`,`FunctionalKey`,`InvisibleUntilUtc`,`Id`),
  CONSTRAINT `FK_SubscriptionEvent_EventPayload` FOREIGN KEY (`PayloadId`) REFERENCES `eventpayload` (`Id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `FK_SubscriptionEvent_Subscription` FOREIGN KEY (`SubscriptionId`) REFERENCES `subscription` (`Id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `FK_SubscriptionEvent_TopicEvent` FOREIGN KEY (`TopicEventId`) REFERENCES `topicevent` (`Id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `topic`
--

DROP TABLE IF EXISTS `topic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `topic` (
  `Id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `Name` varchar(50) CHARACTER SET utf8mb4 NOT NULL,
  `Notes` varchar(500) CHARACTER SET utf8mb4 DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `UK_Topic_Name` (`Name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `topicevent`
--

DROP TABLE IF EXISTS `topicevent`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `topicevent` (
  `Id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `TopicId` bigint(20) unsigned NOT NULL,
  `EventName` varchar(50) CHARACTER SET utf8mb4 DEFAULT NULL,
  `PublicationDateUtc` datetime(6) NOT NULL,
  `ExpirationDateUtc` datetime(6) DEFAULT NULL,
  `FunctionalKey` varchar(50) CHARACTER SET utf8mb4 DEFAULT NULL,
  `Headers` varchar(1000) CHARACTER SET utf8mb4 DEFAULT NULL COMMENT 'Json-formatted key-value pair. Only used for topic-subscription filtering.',
  `Priority` int(11) NOT NULL,
  `PayloadId` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`Id`),
  KEY `FK_TopicEvent_EventPayload` (`PayloadId`),
  CONSTRAINT `FK_TopicEvent_EventPayload` FOREIGN KEY (`PayloadId`) REFERENCES `eventpayload` (`Id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `topicsubscription`
--

DROP TABLE IF EXISTS `topicsubscription`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `topicsubscription` (
  `Id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `TopicId` bigint(20) unsigned NOT NULL,
  `SubscriptionId` bigint(20) unsigned NOT NULL,
  `Enabled` tinyint(1) NOT NULL,
  `Filtered` tinyint(1) NOT NULL,
  PRIMARY KEY (`Id`),
  KEY `FK_TopicSubscription_Subscription` (`SubscriptionId`),
  KEY `FK_TopicSubscription_Topic` (`TopicId`),
  CONSTRAINT `FK_TopicSubscription_Subscription` FOREIGN KEY (`SubscriptionId`) REFERENCES `subscription` (`Id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `FK_TopicSubscription_Topic` FOREIGN KEY (`TopicId`) REFERENCES `topic` (`Id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `topicsubscriptionfilter`
--

DROP TABLE IF EXISTS `topicsubscriptionfilter`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `topicsubscriptionfilter` (
  `Id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `TopicSubscriptionId` bigint(20) unsigned NOT NULL,
  `Header` varchar(100) CHARACTER SET utf8mb4 NOT NULL,
  `MatchExpression` varchar(100) CHARACTER SET utf8mb4 NOT NULL,
  `NotMatch` tinyint(1) NOT NULL,
  PRIMARY KEY (`Id`),
  KEY `FK_TopicSubscriptionFilter_TopicSubscription` (`TopicSubscriptionId`),
  CONSTRAINT `FK_TopicSubscriptionFilter_TopicSubscription` FOREIGN KEY (`TopicSubscriptionId`) REFERENCES `topicsubscription` (`Id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-10-21 12:21:25
