-- List all subscriptions and their details (Results-tab)
declare @start datetime2(7)
declare @end datetime2(7)
select @start = '1900/1/1', @end = SYSUTCDATETIME();

select s.Id, s.Name
,(select count(*) from subscriptionevent se where se.SubscriptionId = s.Id and se.PublicationDateUtc >= @start) as 'Open'
,(select max(PublicationDateUtc) from subscriptionevent se where se.SubscriptionId = s.Id and se.PublicationDateUtc >= @start and se.PublicationDateUtc <= @end) as 'LastPublishedUtc'
,(select max(DeliveryDateUtc) from subscriptionevent se where se.SubscriptionId = s.Id and se.PublicationDateUtc >= @start and se.PublicationDateUtc <= @end) as 'LastDeliveredUtc'
,(select count(*) from consumedsubscriptionevent cse where cse.SubscriptionId = s.Id and cse.PublicationDateUtc >= @start and cse.DeliveryDateUtc <= @end) as 'Consumed'
,(select max(DeliveryDateUtc) from consumedsubscriptionevent cse where cse.SubscriptionId = s.Id and cse.PublicationDateUtc >= @start and cse.DeliveryDateUtc <= @end) as 'LastConsumedUtc'
,(select count(*) from failedsubscriptionevent  fse where fse.SubscriptionId = s.Id and fse.PublicationDateUtc >= @start and fse.FailedDateUtc <= @end and fse.Reason = 0) as 'FailedUnknown'
,(select count(*) from failedsubscriptionevent  fse where fse.SubscriptionId = s.Id and fse.PublicationDateUtc >= @start and fse.FailedDateUtc <= @end and fse.Reason = 1) as 'FailedExpired'
,(select count(*) from failedsubscriptionevent  fse where fse.SubscriptionId = s.Id and fse.PublicationDateUtc >= @start and fse.FailedDateUtc <= @end and fse.Reason = 2) as 'FailedMaxDeliveriesReached'
,(select count(*) from failedsubscriptionevent  fse where fse.SubscriptionId = s.Id and fse.PublicationDateUtc >= @start and fse.FailedDateUtc <= @end and fse.Reason = 3) as 'FailedOvertaken'
,(select count(*) from failedsubscriptionevent fse where fse.SubscriptionId = s.Id and fse.PublicationDateUtc >= @start and fse.FailedDateUtc <= @end and fse.Reason = 4) as 'FailedOther'
,(select max(FailedDateUtc) from failedsubscriptionevent fse where fse.SubscriptionId = s.Id and fse.PublicationDateUtc >= @start and fse.FailedDateUtc <= @end) as 'LastFailedUtc'
from subscription s
group by s.Id, s.Name;

-- Calculate current throughput (Messages-tab)
declare @startSpeed DateTime2(7)
select @startSpeed = DATEADD(s, -60, SYSUTCDATETIME()) -- Last 60 seconds
declare @totaltime float
declare @totalconsumed float

select @totaltime = convert(float, datediff(ms, min(DeliveryDateUtc), max(ConsumedDateUtc)))
	,@totalconsumed = convert(float, count(*))
from ConsumedSubscriptionEvent where DeliveryDateUtc >= @startSpeed

declare @totalleft int
select @totalleft = count(*) from SubscriptionEvent

print 'Total consumed: ' + convert(varchar(100), @totalconsumed)
print 'Time spent:     ' + convert(varchar(100), @totaltime) + 'ms'
print 'Time per item:  ' + convert(varchar(100), (@totaltime / @totalconsumed)) + 'ms'
print 'Total to go:    ' + convert(varchar(100), @totalleft)
