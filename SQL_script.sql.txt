/*Retrieve data for analysis*/

/*Retrieving User Base*/
drop table `ucl-dissertation-project.ucl_dissertation_dataset.user_base`;

create table `ucl-dissertation-project.ucl_dissertation_dataset.user_base` as
(select cast(user.id as INT64) as user_id, user_phone.phone,user_phone.activatedDate, user_phone.deactivatedDate
FROM  `ucl-dissertation-project.ucl_dissertation_dataset.user` as user
left join 
(select user, REGEXP_REPLACE(phone,'[^0-9 ]','') as phone, 
-- min(case when phone like '%Deleted%' then 1 else 0 end) as deletedInd,
min(activatedDate) as activatedDate, 
max(case when EXTRACT(year from deactivatedDate) = 1969 then CURRENT_TIMESTAMP() else deactivatedDate end) as deactivatedDate,
from `ucl-dissertation-project.ucl_dissertation_dataset.user_phone` 
where length(trim(REGEXP_REPLACE(phone,'[^0-9 ]','')) ) > 0
and EXTRACT (YEAR FROM activatedDate) <> 1969
group by 1,2) as user_phone
on cast(user.id as INT64) = cast(user_phone.user as INT64)
where user.whiteBrand in ('1','2') and user.isTourist = FALSE and user.company = 'nan' and user.isActive = TRUE and user.emailStatus = 'confirmed' and user_phone.phone is not null);

/*Retrieving cdr data*/
drop table `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_data`;
drop table `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_voice`;
drop table `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_sms`;

create table `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_data` as
select user.user_id, user.phone, user.activatedDate, user.deactivatedDate, 
sum(case 
when cdr.OriginalTime between EXTRACT (DATE from user.activatedDate) and EXTRACT (DATE from user.deactivatedDate) then cdr.Volume
else 0 end) as data_usage,
sum(case 
when cdr.OriginalTime between EXTRACT (DATE from user.activatedDate) and EXTRACT (DATE from user.deactivatedDate) then cdr.Duration
else 0 end) as data_usage_duration
from `ucl-dissertation-project.ucl_dissertation_dataset.user_base` as user
left join `ucl-dissertation-project.ucl_dissertation_dataset.cdr_data` as cdr
on user.phone = cdr.MSISDN
group by 1,2,3,4;

create table `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_voice` as
select base.user_id, base.phone, base.activatedDate, base.deactivatedDate,
sum(case 
when cdr_voice.OriginalTime between EXTRACT (DATE from base.activatedDate) and EXTRACT (DATE from base.deactivatedDate) 
then cdr_voice.CallDuration else 0 end) as outgoing_seconds,
count(distinct case 
when cdr_voice.OriginalTime between EXTRACT (DATE from base.activatedDate) and EXTRACT (DATE from base.deactivatedDate) 
then cdr_voice.UseID end) as outgoing_calls
from `ucl-dissertation-project.ucl_dissertation_dataset.user_base` as base
left join `ucl-dissertation-project.ucl_dissertation_dataset.cdr_voice` as cdr_voice
on base.phone = cdr_voice.MSISDN
group by 1,2,3,4;

create table `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_sms` as
select base.user_id, base.phone, base.activatedDate, base.deactivatedDate,
count(distinct case 
when cdr_sms.OriginalTime between EXTRACT (DATE from base.activatedDate) and EXTRACT (DATE from base.deactivatedDate) 
then cdr_sms.UseID end) as outgoing_sms
from `ucl-dissertation-project.ucl_dissertation_dataset.user_base` as base
left join `ucl-dissertation-project.ucl_dissertation_dataset.cdr_sms` as cdr_sms
on base.phone = cdr_sms.MSISDN
group by 1,2,3,4;

/*Retrieving Plan Details*/
drop table `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_plan_v1`;
drop table `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_plan`;

create table `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_plan_v1` as
select distinct cast(user as INT64) as user, REGEXP_REPLACE(phone,'[^0-9 ]','') as phone, 
CAST(CAST(pricePlanID AS float64) AS INT64) as pricePlanID
from `ucl-dissertation-project.ucl_dissertation_dataset.user_phone` 
where length(trim(REGEXP_REPLACE(phone,'[^0-9 ]','')) ) > 0
and EXTRACT (YEAR FROM activatedDate) <> 1969
and pricePlanID <> '0.0' and pricePlanID is not null;


insert into `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_plan_v1`
select distinct cast(user as INT64), REGEXP_REPLACE(phone,'[^0-9 ]',''), 
CAST(CAST(bundleIds1 AS float64) AS INT64)
from `ucl-dissertation-project.ucl_dissertation_dataset.user_phone` 
where length(trim(REGEXP_REPLACE(phone,'[^0-9 ]','')) ) > 0
and EXTRACT (YEAR FROM activatedDate) <> 1969
and trim(bundleIds1) <> '' and trim(bundleIds1) is not null;

insert into `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_plan_v1`
select distinct cast(user as INT64), REGEXP_REPLACE(phone,'[^0-9 ]',''), 
CAST(CAST(bundleIds2 AS float64) AS INT64)
from `ucl-dissertation-project.ucl_dissertation_dataset.user_phone` 
where length(trim(REGEXP_REPLACE(phone,'[^0-9 ]','')) ) > 0
and EXTRACT (YEAR FROM activatedDate) <> 1969
and trim(bundleIds2) <> '' and trim(bundleIds2) is not null;

insert into `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_plan_v1`
select distinct cast(user as INT64), REGEXP_REPLACE(phone,'[^0-9 ]',''), 
CAST(CAST(case when bundleIds3 like '%,%' then split(bundleIds3,',') [offset(0)] else bundleIds3 end AS float64) AS INT64)
from `ucl-dissertation-project.ucl_dissertation_dataset.user_phone` 
where length(trim(REGEXP_REPLACE(phone,'[^0-9 ]','')) ) > 0
and EXTRACT (YEAR FROM activatedDate) <> 1969
and trim(bundleIds3) <> '' and trim(bundleIds3) is not null;

insert into `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_plan_v1`
select distinct cast(user as INT64), REGEXP_REPLACE(phone,'[^0-9 ]',''), 
CAST(CAST(case when bundleIds3 like '%,%' then split(bundleIds3,',') [offset(1)] else '' end AS float64) AS INT64)
from `ucl-dissertation-project.ucl_dissertation_dataset.user_phone` 
where length(trim(REGEXP_REPLACE(phone,'[^0-9 ]','')) ) > 0
and EXTRACT (YEAR FROM activatedDate) <> 1969
and trim(bundleIds3) <> '' and trim(bundleIds3) is not null and bundleIds3 like '%,%';

create table `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_plan` as
select distinct user, count(distinct pricePlanID) number_of_plans,
round(avg(b.validity),2) as avg_validity,
round(avg(b.voice_quantity),2) as avg_voice_quantity,
round(avg(b.data_quantity),2) as avg_data_quantity,
round(avg(b.sms_quantity),2) as avg_sms_quantity,
max(b.fb_quantity) as max_fb_quantity,
max(b.social_unlimited) as max_social_unlimited,
round(avg(case when b.Price = 999999 then 0 else b.Price end),2) as avg_Price,
from `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_plan_v1` as a
left join (select * from `ucl-dissertation-project.ucl_dissertation_dataset.Plan_Details` where validity <> 999999) as b
on a.pricePlanID = b.plan_id
where a.user in (select distinct user_id from `ucl-dissertation-project.ucl_dissertation_dataset.user_base`)
group by 1
order by 1;

/*Retrieving Transaction Details*/
drop table `ucl-dissertation-project.ucl_dissertation_dataset.user_transaction_details`;
drop table `ucl-dissertation-project.ucl_dissertation_dataset.user_transaction_details_v1`;

create table `ucl-dissertation-project.ucl_dissertation_dataset.user_transaction_details` as
select cast(cast(user as FLOAT64) as INT64) AS user, 
case when paymentConcept = 1 then 1 else 0 end as Is_recharge_to_mobile_number,
case when paymentConcept = 2 then 1 else 0 end as Is_Bundle_Purchase,
case when paymentConcept = 7 then 1 else 0 end as Is_Recharge_to_the_universal_wallet,
round(avg(amount),2) as average_transaction_amount, 
round(avg(amountBalance),2) as average_balance_in_account
from `ucl-dissertation-project.ucl_dissertation_dataset.transaction_copy` 
where transactionId is not null
and user <> 'nan'
and cast(cast(user as FLOAT64) as INT64) in (select distinct user_id from `ucl-dissertation-project.ucl_dissertation_dataset.user_base` )
and isCancelPayment = 'False'
group by 1,2,3,4;

create table `ucl-dissertation-project.ucl_dissertation_dataset.user_transaction_details_v1` as
select user, 
max(Is_recharge_to_mobile_number) as Is_recharge_to_mobile_number,
max(Is_Bundle_Purchase) as Is_Bundle_Purchase,
max(Is_Recharge_to_the_universal_wallet) as Is_Recharge_to_the_universal_wallet,
avg(average_transaction_amount) as average_transaction_amount, 
avg(average_balance_in_account) as average_balance_in_account
from `ucl-dissertation-project.ucl_dissertation_dataset.user_transaction_details` 
group by 1;

/*Summarized dataset creation*/
drop table `ucl-dissertation-project.ucl_dissertation_dataset.user_summary`;
drop table `ucl-dissertation-project.ucl_dissertation_dataset.user_summary_v1`;
drop table `ucl-dissertation-project.ucl_dissertation_dataset.user_summary_final`;

create table `ucl-dissertation-project.ucl_dissertation_dataset.user_summary_v1` as
select user.user_id, count(distinct user.phone) as number_of_phones, 
min(user.activatedDate) as user_acquired_date,
max(user.deactivatedDate) as user_deactivated_date,
sum(cdr_data.data_usage) as data_usage, sum(cdr_data.data_usage_duration) as data_usage_duration,
sum(cdr_voice.outgoing_seconds) as outgoing_seconds, sum(cdr_voice.outgoing_calls) as outgoing_calls,
sum(cdr_sms.outgoing_sms) as outgoing_sms
from `ucl-dissertation-project.ucl_dissertation_dataset.user_base` as user
left join
(select user_id, sum(data_usage) as data_usage, sum(data_usage_duration) as data_usage_duration, 
from `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_data` 
group by 1) as cdr_data
on user.user_id = cdr_data.user_id
left join 
(select user_id, sum(outgoing_seconds) as outgoing_seconds, sum(outgoing_calls) as outgoing_calls, 
from `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_voice` 
group by 1) as cdr_voice
on user.user_id = cdr_voice.user_id
left join 
(select user_id, sum(outgoing_sms) as outgoing_sms
from `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_sms` 
group by 1) as cdr_sms
on user.user_id = cdr_sms.user_id
group by 1;

create table `ucl-dissertation-project.ucl_dissertation_dataset.user_summary` as
select a.*, 
round((date_diff(a.user_deactivated_date,a.user_acquired_date, DAY))/30,2) as tenure,
plan_dets.*, 
transaction_dets.Is_recharge_to_mobile_number, 
transaction_dets.Is_Bundle_Purchase, 
transaction_dets.Is_Recharge_to_the_universal_wallet, transaction_dets.average_balance_in_account, transaction_dets. average_transaction_amount
from `ucl-dissertation-project.ucl_dissertation_dataset.user_summary_v1` as a
left join `ucl-dissertation-project.ucl_dissertation_dataset.user_phone_plan` as plan_dets
on a.user_id = plan_dets.user
left join `ucl-dissertation-project.ucl_dissertation_dataset.user_transaction_details_v1` as transaction_dets
on a.user_id = transaction_dets.user;

ALTER TABLE `ucl-dissertation-project.ucl_dissertation_dataset.user_summary`
DROP COLUMN IF EXISTS user,
DROP COLUMN IF EXISTS user_acquired_date,
DROP COLUMN IF EXISTS user_deactivated_date,
DROP COLUMN IF EXISTS Is_Recharge_to_the_universal_wallet;

create table `ucl-dissertation-project.ucl_dissertation_dataset.user_summary_final` as
select a.*,
round(data_usage/(tenure*number_of_phones*power(1024,3)),6) as average_data_usage,
round(data_usage_duration/(tenure*number_of_phones*power(60,2)),6) as average_data_usage_duration,
round(outgoing_seconds/(tenure*number_of_phones*power(60,2)),6) as average_outgoing_seconds, 
round(outgoing_calls/(tenure*number_of_phones),6) as average_outgoing_calls,
round(outgoing_sms/(tenure*number_of_phones),6) as average_outgoing_sms,
case when b.district = 'nan' then 'Missing'
when c.UBIGEO is null then 'Others'
else c.Zone end as Zone, 
case when b.gender = 'nan' then 'Missing'
when b.gender = '1.0' then 'Male'
else 'Female' end as gender,
case when trim(b.birthday) is null or trim(b.birthday) = '' then null
when CAST(b.birthday AS DATE) > CURRENT_DATE() then null
else DATE_DIFF(CURRENT_DATE(),CAST(b.birthday AS DATE), YEAR) - 
(IF(EXTRACT(MONTH FROM CAST(b.birthday AS DATE))*100 + EXTRACT(DAY FROM CAST(b.birthday AS DATE)) > EXTRACT(MONTH FROM CURRENT_DATE())*100 + EXTRACT(DAY FROM CURRENT_DATE()),1,0)) end as age
from `ucl-dissertation-project.ucl_dissertation_dataset.user_summary` as a
left join `ucl-dissertation-project.ucl_dissertation_dataset.user` as b
on a.user_id = cast(b.id as INT64)
left join `ucl-dissertation-project.ucl_dissertation_dataset.District_Zone_Mapping` as c
on regexp_extract( b.district, r'^[^.]*') = c.UBIGEO
where tenure <> 0 and number_of_plans is not null;

