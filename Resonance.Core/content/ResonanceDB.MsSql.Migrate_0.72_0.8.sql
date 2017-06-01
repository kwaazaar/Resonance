-- Migratie MsSQL from DB-model v0.72 to v0.8
ALTER TABLE dbo.ConsumedSubscriptionEvent ADD
	DeliveryCount int NULL
GO
update ConsumedSubscriptionEvent
set DeliveryCount = 0
GO
ALTER TABLE dbo.ConsumedSubscriptionEvent
ALTER COLUMN DeliveryCount int NOT NULL
go

ALTER TABLE dbo.FailedSubscriptionEvent ADD
	DeliveryCount int NULL
GO
update FailedSubscriptionEvent
set DeliveryCount = 0
GO
ALTER TABLE dbo.FailedSubscriptionEvent
ALTER COLUMN DeliveryCount int NOT NULL
go

ALTER TABLE dbo.Subscription ADD
	LogConsumed bit NULL,
	LogFailed bit NULL
	GO
UPDATE dbo.Subscription
SET LogConsumed = 0x1, LogFailed = 0x1
GO
ALTER TABLE dbo.Subscription
ALTER COLUMN LogConsumed bit NOT NULL
go
ALTER TABLE dbo.Subscription
ALTER COLUMN LogFailed bit NOT NULL
go

ALTER TABLE dbo.Topic ADD
	Log bit NULL
GO
UPDATE dbo.Topic
SET Log = 0x1
GO
ALTER TABLE dbo.Topic
ALTER COLUMN Log bit NOT NULL
go

ALTER TABLE [dbo].[SubscriptionEvent] DROP CONSTRAINT [FK_SubscriptionEvent_TopicEvent]
GO
ALTER TABLE dbo.SubscriptionEvent
ALTER COLUMN TopicEventId bigint NULL
go
