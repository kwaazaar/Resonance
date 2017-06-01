-- Migratie MySQL from DB-model v0.72 to v0.8
ALTER TABLE `consumedsubscriptionevent` 
ADD COLUMN `DeliveryCount` INT(11) NULL AFTER `ConsumedDateUtc`;
update consumedsubscriptionevent
set DeliveryCount = 1
where id > 0 and subscriptionid > 0;
ALTER TABLE `consumedsubscriptionevent` 
CHANGE COLUMN `DeliveryCount` `DeliveryCount` INT(11) NOT NULL;

ALTER TABLE `failedsubscriptionevent` 
ADD COLUMN `DeliveryCount` INT(11) NULL AFTER `ReasonOther`;
update failedsubscriptionevent
set DeliveryCount = 1
where id > 0 and subscriptionid > 0;
ALTER TABLE `failedsubscriptionevent` 
CHANGE COLUMN `DeliveryCount` `DeliveryCount` INT(11) NOT NULL;

ALTER TABLE `subscription` 
ADD COLUMN `LogConsumed` TINYINT(1) NULL AFTER `DeliveryDelay`,
ADD COLUMN `LogFailed` TINYINT(1) NULL AFTER `LogConsumed`;
update subscription
set LogConsumed = 1, LogFailed = 1
where id > 0;
ALTER TABLE `subscription` 
CHANGE COLUMN `LogConsumed` `LogConsumed` TINYINT(1) NOT NULL;
ALTER TABLE `subscription` 
CHANGE COLUMN `LogFailed` `LogFailed` TINYINT(1) NOT NULL;

ALTER TABLE `topic` 
ADD COLUMN `Log` TINYINT(1) NULL AFTER `Notes`;
update topic
set Log = 1
where id > 0;
ALTER TABLE `topic` 
CHANGE COLUMN `Log` `Log` TINYINT(1) NOT NULL;

ALTER TABLE `subscriptionevent` 
DROP FOREIGN KEY `FK_SubscriptionEvent_TopicEvent`;
ALTER TABLE `subscriptionevent` 
CHANGE COLUMN `TopicEventId` `TopicEventId` BIGINT(20) UNSIGNED NULL DEFAULT NULL ;
