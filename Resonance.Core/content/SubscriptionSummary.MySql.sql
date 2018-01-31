-- Calculate current throughput (Messages-tab)
select	min(deliverydateutc) as 'Start',
		max(ConsumedDateUtc) as 'Finish',
		(TIMESTAMPDIFF(MICROSECOND,min(deliverydateutc),max(ConsumedDateUtc)) / 1000) as 'Totaltime',
		count(*) as 'Total items',
        ((TIMESTAMPDIFF(MICROSECOND,min(deliverydateutc),max(ConsumedDateUtc)) / 1000) / count(*)) as 'Time/item (ms)'
from consumedsubscriptionevent
where TIMESTAMPDIFF(SECOND,deliverydateutc, UTC_TIMESTAMP()) < 60; -- Last 60 seconds


-- List all subscriptions and their details
set @end = utc_timestamp();
set @start = '1900/1/1';

select s.Id, s.Name
,(select count(*) from subscriptionevent se where se.SubscriptionId = s.Id and se.PublicationDateUtc >= @start) as 'Open'
,(select max(PublicationDateUtc) from subscriptionevent se where se.SubscriptionId = s.Id and se.PublicationDateUtc >= @start and se.PublicationDateUtc <= @end) as 'LastPublishedUtc'
,(select max(DeliveryDateUtc) from subscriptionevent se where se.SubscriptionId = s.Id and se.PublicationDateUtc >= @start and se.PublicationDateUtc <= @end) as 'LastDeliveryUtc'
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