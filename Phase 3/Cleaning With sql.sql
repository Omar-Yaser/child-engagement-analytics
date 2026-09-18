drop database if exists child_engagement_analytics_raw;
create database child_engagement_analytics_raw
    character set utf8mb4 collate utf8mb4_unicode_ci;
use child_engagement_analytics_raw;


create table organization (
    org_id  varchar(50),
    name    varchar(255),
    type    varchar(100)
) engine=innodb;

create table child (
    child_id              varchar(50),
    fname                 varchar(255),
    lname                 varchar(255),
    age                   varchar(50),
    family_size           varchar(50),
    num_of_siblings       varchar(50),
    parents_working_hours varchar(50),
    household_income      varchar(100),
    access_to_technology  varchar(50),
    number_of_devices     varchar(50),
    environment_type      varchar(100),
    school_type           varchar(100)
) engine=innodb;

create table preferences (
    preference_id varchar(50),
    name          varchar(255),
    category      varchar(100)
) engine=innodb;

create table activity (
    activity_id        varchar(50),
    org_id             varchar(50),
    name               varchar(255),
    duration           varchar(50),
    group_size         varchar(50),
    interaction_type   varchar(100),
    activity_format    varchar(100),
    participation_type varchar(100),
    difficulty_level   varchar(100),
    movement_required  varchar(50)
) engine=innodb;

create table child_organization (
    child_id        varchar(50),
    org_id          varchar(50),
    enrollment_date varchar(100)
) engine=innodb;

create table child_preference (
    child_id      varchar(50),
    preference_id varchar(50)
) engine=innodb;

create table lifestyle (
    child_id           varchar(50),
    sleep_duration     varchar(50),
    screen_time        varchar(50),
    gaming             varchar(50),
    social_media_usage varchar(50),
    outdoor_time       varchar(50),
    study_time         varchar(50),
    free_play          varchar(50)
) engine=innodb;

create table engagement (
    session_id                 varchar(50),
    child_id                   varchar(50),
    activity_id                varchar(50),
    session_date               varchar(100),
    avg_attention_duration     varchar(50),
    response_time              varchar(50),
    task_completion_rate       varchar(50),
    engagement_score           varchar(50),
    number_of_attention_shifts varchar(50)
) engine=innodb;

create table data_quality_report (
    section                varchar(50),
    table_name             varchar(100),
    total_rows             varchar(50),
    total_columns          varchar(50),
    missing_values         varchar(50),
    columns_with_missing   varchar(50),
    duplicate_records      varchar(50),
    invalid_values         varchar(50),
    foreign_key_violations varchar(50),
    problem_id             varchar(50),
    problem_name           varchar(255),
    tables_affected        varchar(255),
    records_affected       varchar(50),
    details                text
) engine=innodb;


----------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

load data local infile "D:\AI and data analysis\NTI\project_final_data\data_creation_with_python\output\child_organization.csv"
into table organization
fields terminated by ',' optionally enclosed by '"' escaped by '"'
lines terminated by '\n'
ignore 1 lines
(@org_id, @name, @type)
set org_id = nullif(@org_id, ''),
    name   = nullif(@name, ''),
    type   = nullif(@type, '');

load data local infile "D:\AI and data analysis\NTI\project_final_data\data_creation_with_python\output\child.csv"
into table child
fields terminated by ',' optionally enclosed by '"' escaped by '"'
lines terminated by '\n'
ignore 1 lines
(@child_id, @fname, @lname, @age, @family_size, @num_of_siblings,
 @parents_working_hours, @household_income, @access_to_technology,
 @number_of_devices, @environment_type, @school_type)
set child_id              = nullif(@child_id, ''),
    fname                 = nullif(@fname, ''),
    lname                 = nullif(@lname, ''),
    age                   = nullif(@age, ''),
    family_size           = nullif(@family_size, ''),
    num_of_siblings       = nullif(@num_of_siblings, ''),
    parents_working_hours = nullif(@parents_working_hours, ''),
    household_income      = nullif(@household_income, ''),
    access_to_technology  = nullif(@access_to_technology, ''),
    number_of_devices     = nullif(@number_of_devices, ''),
    environment_type      = nullif(@environment_type, ''),
    school_type           = nullif(@school_type, '');

load data local infile 'c:/path/to/output/preferences.csv'
into table preferences
fields terminated by ',' optionally enclosed by '"' escaped by '"'
lines terminated by '\n'
ignore 1 lines
(@preference_id, @name, @category)
set preference_id = nullif(@preference_id, ''),
    name          = nullif(@name, ''),
    category      = nullif(@category, '');

load data local infile "D:\AI and data analysis\NTI\project_final_data\data_creation_with_python\output\activity.csv"
into table activity
fields terminated by ',' optionally enclosed by '"' escaped by '"'
lines terminated by '\n'
ignore 1 lines
(@activity_id, @org_id, @name, @duration, @group_size, @interaction_type,
 @activity_format, @participation_type, @difficulty_level, @movement_required)
set activity_id        = nullif(@activity_id, ''),
    org_id             = nullif(@org_id, ''),
    name               = nullif(@name, ''),
    duration           = nullif(@duration, ''),
    group_size         = nullif(@group_size, ''),
    interaction_type   = nullif(@interaction_type, ''),
    activity_format    = nullif(@activity_format, ''),
    participation_type = nullif(@participation_type, ''),
    difficulty_level   = nullif(@difficulty_level, ''),
    movement_required  = nullif(@movement_required, '');

load data local infile 'c:/path/to/output/child_organization.csv'
into table child_organization
fields terminated by ',' optionally enclosed by '"' escaped by '"'
lines terminated by '\n'
ignore 1 lines
(@child_id, @org_id, @enrollment_date)
set child_id        = nullif(@child_id, ''),
    org_id          = nullif(@org_id, ''),
    enrollment_date = nullif(@enrollment_date, '');

load data local infile "D:\AI and data analysis\NTI\project_final_data\data_creation_with_python\output\child_organization.csv"
into table child_preference
fields terminated by ',' optionally enclosed by '"' escaped by '"'
lines terminated by '\n'
ignore 1 lines
(@child_id, @preference_id)
set child_id      = nullif(@child_id, ''),
    preference_id = nullif(@preference_id, '');

load data local infile "D:\AI and data analysis\NTI\project_final_data\data_creation_with_python\output\lifestyle.csv"
into table lifestyle
fields terminated by ',' optionally enclosed by '"' escaped by '"'
lines terminated by '\n'
ignore 1 lines
(@child_id, @sleep_duration, @screen_time, @gaming, @social_media_usage,
 @outdoor_time, @study_time, @free_play)
set child_id           = nullif(@child_id, ''),
    sleep_duration     = nullif(@sleep_duration, ''),
    screen_time        = nullif(@screen_time, ''),
    gaming             = nullif(@gaming, ''),
    social_media_usage = nullif(@social_media_usage, ''),
    outdoor_time       = nullif(@outdoor_time, ''),
    study_time         = nullif(@study_time, ''),
    free_play          = nullif(@free_play, '');

load data local infile "D:\AI and data analysis\NTI\project_final_data\data_creation_with_python\output\engagement.csv"
into table engagement
fields terminated by ',' optionally enclosed by '"' escaped by '"'
lines terminated by '\n'
ignore 1 lines
(@session_id, @child_id, @activity_id, @session_date, @avg_attention_duration,
 @response_time, @task_completion_rate, @engagement_score, @number_of_attention_shifts)
set session_id                 = nullif(@session_id, ''),
    child_id                   = nullif(@child_id, ''),
    activity_id                = nullif(@activity_id, ''),
    session_date               = nullif(@session_date, ''),
    avg_attention_duration     = nullif(@avg_attention_duration, ''),
    response_time              = nullif(@response_time, ''),
    task_completion_rate       = nullif(@task_completion_rate, ''),
    engagement_score           = nullif(@engagement_score, ''),
    number_of_attention_shifts = nullif(@number_of_attention_shifts, '');

load data local infile "D:\AI and data analysis\NTI\project_final_data\data_creation_with_python\output\data_quality_report.csv"
into table data_quality_report
fields terminated by ',' optionally enclosed by '"' escaped by '"'
lines terminated by '\n'
ignore 1 lines
(@section, @table_name, @total_rows, @total_columns, @missing_values,
 @columns_with_missing, @duplicate_records, @invalid_values,
 @foreign_key_violations, @problem_id, @problem_name, @tables_affected,
 @records_affected, @details)
set section                = nullif(@section, ''),
    table_name             = nullif(@table_name, ''),
    total_rows             = nullif(@total_rows, ''),
    total_columns          = nullif(@total_columns, ''),
    missing_values         = nullif(@missing_values, ''),
    columns_with_missing   = nullif(@columns_with_missing, ''),
    duplicate_records      = nullif(@duplicate_records, ''),
    invalid_values         = nullif(@invalid_values, ''),
    foreign_key_violations = nullif(@foreign_key_violations, ''),
    problem_id             = nullif(@problem_id, ''),
    problem_name           = nullif(@problem_name, ''),
    tables_affected        = nullif(@tables_affected, ''),
    records_affected       = nullif(@records_affected, ''),
    details                = nullif(@details, '');


----------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

-- create indexes to improve performance

create index ix_stg_child_id       on child (child_id);
create index ix_stg_act_id         on activity (activity_id);
create index ix_stg_act_org        on activity (org_id);
create index ix_stg_eng_child      on engagement (child_id);
create index ix_stg_eng_activity   on engagement (activity_id);
create index ix_stg_co_child       on child_organization (child_id);
create index ix_stg_co_org         on child_organization (org_id);
create index ix_stg_cp_child       on child_preference (child_id);
create index ix_stg_cp_pref        on child_preference (preference_id);
create index ix_stg_life_child     on lifestyle (child_id);



----------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

-- to show how many rows were inserted into each table

select 'organization'       as table_name, count(*) as rows_loaded from organization
union all select 'child',              count(*) from child
union all select 'preferences',        count(*) from preferences
union all select 'activity',           count(*) from activity
union all select 'child_organization', count(*) from child_organization
union all select 'child_preference',   count(*) from child_preference
union all select 'lifestyle',          count(*) from lifestyle
union all select 'engagement',         count(*) from engagement;


insert into child_engagement_analytics.organization (org_id, name, type)
select distinct
       cast(s.org_id as unsigned),
       trim(s.name),
       nullif(trim(s.type), '')
from   child_engagement_analytics_raw.organization s
where  s.org_id regexp '^[0-9]+$'
  and  s.name is not null
group by cast(s.org_id as unsigned), trim(s.name), nullif(trim(s.type), '');


-- ---------------------------------------------------------------- preferences

insert into child_engagement_analytics.preferences (preference_id, name, category)
select cast(s.preference_id as unsigned),
       trim(s.name),
       nullif(trim(s.category), '')
from   child_engagement_analytics_raw.preferences s
where  s.preference_id regexp '^[0-9]+$'
  and  s.name is not null
group by cast(s.preference_id as unsigned);


-- ---------------------------------------------------------------- child
insert into child_engagement_analytics.child
    (child_id, fname, lname, age, family_size, num_of_siblings,
     parents_working_hours, household_income, access_to_technology,
     number_of_devices, environment_type, school_type)
select cast(s.child_id as unsigned),
       nullif(trim(s.fname), ''),
       nullif(trim(s.lname), ''),
       cast(s.age as signed),
       case when s.family_size     regexp '^[0-9]+$' then cast(s.family_size as signed) end,
       case when s.num_of_siblings regexp '^[0-9]+$' then cast(s.num_of_siblings as signed) end,
       case when s.parents_working_hours regexp '^[0-9]+(\\.[0-9]+)?$'
            then cast(s.parents_working_hours as decimal(4,1)) end,
       -- strips '$', thousands separators and text such as 'egp'
       case when replace(replace(replace(lower(s.household_income), '$',''), ',',''), ' egp','')
                 regexp '^[0-9]+(\\.[0-9]+)?$'
            then cast(replace(replace(replace(lower(s.household_income), '$',''), ',',''), ' egp','')
                      as decimal(10,2)) end,
       case when s.access_to_technology in ('0','1') then cast(s.access_to_technology as unsigned) end,
       case when s.number_of_devices regexp '^[0-9]+$' then cast(s.number_of_devices as signed) end,
       case lower(trim(s.environment_type))
            when 'urban' then 'Urban' when 'urb' then 'Urban' when 'city' then 'Urban'
            when 'suburban' then 'Suburban' when 'sub' then 'Suburban'
            when 'sub-urban' then 'Suburban' when 'suburb' then 'Suburban'
            when 'rural' then 'Rural' when 'rur' then 'Rural'
            when 'village' then 'Rural' when 'countryside' then 'Rural'
       end,
       case lower(trim(s.school_type))
            when 'public' then 'Public' when 'pub' then 'Public'
            when 'gov' then 'Public' when 'governmental' then 'Public'
            when 'private' then 'Private' when 'priv' then 'Private' when 'pvt' then 'Private'
            when 'homeschool' then 'Homeschool' when 'home school' then 'Homeschool'
            when 'home-school' then 'Homeschool' when 'hs' then 'Homeschool'
            when 'international' then 'International' when 'intl' then 'International'
            when 'int.' then 'International'
       end
from   child_engagement_analytics_raw.child s
where  s.child_id regexp '^[0-9]+$'
  and  s.age regexp '^[0-9]+$'
  and  cast(s.age as signed) between 3 and 18          -- drops the check-constraint breakers
group by cast(s.child_id as unsigned);                 -- drops the duplicated primary keys


-- ---------------------------------------------------------------- activity
insert into child_engagement_analytics.activity
    (activity_id, org_id, name, duration, group_size, interaction_type,
     activity_format, participation_type, difficulty_level, movement_required)
select cast(s.activity_id as unsigned),
       cast(s.org_id as unsigned),
       coalesce(nullif(trim(s.name), ''), 'unknown activity'),
       case when s.duration regexp '^[0-9]+(\\.[0-9]+)?$'
                 and cast(s.duration as decimal(10,2)) > 0
            then cast(s.duration as signed) end,
       case when s.group_size regexp '^[0-9]+$' then cast(s.group_size as signed) end,
       case when lower(trim(s.interaction_type)) in ('individual','indiv','ind') then 'Individual'
            when lower(trim(s.interaction_type)) in ('pair','pairs','pr','2-person') then 'Pair'
            when lower(trim(s.interaction_type)) in ('group','grp','group work') then 'Group' end,
       case when lower(trim(s.activity_format)) in ('digital','online','dig') then 'Digital'
            when lower(trim(s.activity_format)) in ('physical','offline','in-person','phy') then 'Physical'
            when lower(trim(s.activity_format)) in ('hybrid','blended','mixed','hyb') then 'Hybrid' end,
       case when lower(trim(s.participation_type)) in ('voluntary','opt-in','opt in','vol') then 'Voluntary'
            when lower(trim(s.participation_type)) in ('assigned','mandatory','asg') then 'Assigned' end,
       case when lower(trim(s.difficulty_level)) in ('easy','low','level 1','e') then 'Easy'
            when lower(trim(s.difficulty_level)) in ('medium','med','moderate','level 2') then 'Medium'
            when lower(trim(s.difficulty_level)) in ('hard','high','difficult','level 3') then 'Hard' end,
       case when s.movement_required in ('0','1') then cast(s.movement_required as unsigned) end
from   child_engagement_analytics_raw.activity s
where  s.activity_id regexp '^[0-9]+$'
  and  s.org_id regexp '^[0-9]+$'
  and  cast(s.org_id as unsigned) in (select org_id from child_engagement_analytics.organization)
group by cast(s.activity_id as unsigned);


-- ---------------------------------------------------------------- child_organization
insert into child_engagement_analytics.child_organization (child_id, org_id, enrollment_date)
select cast(s.child_id as unsigned),
       cast(s.org_id as unsigned),
       min(case when s.enrollment_date regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                then str_to_date(s.enrollment_date, '%Y-%m-%d') end)
from   child_engagement_analytics_raw.child_organization s
where  s.child_id regexp '^[0-9]+$'
  and  s.org_id regexp '^[0-9]+$'
  and  cast(s.child_id as unsigned) in (select child_id from child_engagement_analytics.child)
  and  cast(s.org_id   as unsigned) in (select org_id   from child_engagement_analytics.organization)
group by cast(s.child_id as unsigned), cast(s.org_id as unsigned);


-- ---------------------------------------------------------------- child_preference
insert into child_engagement_analytics.child_preference (child_id, preference_id)
select distinct cast(s.child_id as unsigned), cast(s.preference_id as unsigned)
from   child_engagement_analytics_raw.child_preference s
where  s.child_id regexp '^[0-9]+$'
  and  s.preference_id regexp '^[0-9]+$'
  and  cast(s.child_id as unsigned)      in (select child_id      from child_engagement_analytics.child)
  and  cast(s.preference_id as unsigned) in (select preference_id from child_engagement_analytics.preferences);


-- ---------------------------------------------------------------- lifestyle
insert into child_engagement_analytics.lifestyle
    (child_id, sleep_duration, screen_time, gaming, social_media_usage,
     outdoor_time, study_time, free_play)
select cast(s.child_id as unsigned),
       max(case when s.sleep_duration     regexp '^[0-9]+(\\.[0-9]+)?$' and cast(s.sleep_duration as decimal(10,2)) < 100
                then cast(s.sleep_duration as decimal(3,1)) end),
       max(case when s.screen_time        regexp '^[0-9]+(\\.[0-9]+)?$' and cast(s.screen_time as decimal(10,2)) < 100
                then cast(s.screen_time as decimal(3,1)) end),
       max(case when s.gaming             regexp '^[0-9]+(\\.[0-9]+)?$' and cast(s.gaming as decimal(10,2)) < 100
                then cast(s.gaming as decimal(3,1)) end),
       max(case when s.social_media_usage regexp '^[0-9]+(\\.[0-9]+)?$' and cast(s.social_media_usage as decimal(10,2)) < 100
                then cast(s.social_media_usage as decimal(3,1)) end),
       max(case when s.outdoor_time       regexp '^[0-9]+(\\.[0-9]+)?$' then cast(s.outdoor_time as decimal(3,1)) end),
       max(case when s.study_time         regexp '^[0-9]+(\\.[0-9]+)?$' then cast(s.study_time as decimal(3,1)) end),
       max(case when s.free_play          regexp '^[0-9]+(\\.[0-9]+)?$' then cast(s.free_play as decimal(3,1)) end)
from   child_engagement_analytics_raw.lifestyle s
where  s.child_id regexp '^[0-9]+$'
  and  cast(s.child_id as unsigned) in (select child_id from child_engagement_analytics.child)
group by cast(s.child_id as unsigned);
-- note: the '< 100' guards drop the rows that were logged in minutes instead of hours.
--       decide yourself whether to divide them by 60 instead of discarding them.


-- ---------------------------------------------------------------- engagement
insert into child_engagement_analytics.engagement
    (session_id, child_id, activity_id, session_date, avg_attention_duration,
     response_time, task_completion_rate, engagement_score, number_of_attention_shifts)
select cast(s.session_id as unsigned),
       cast(s.child_id as unsigned),
       cast(s.activity_id as unsigned),
       max(case when s.session_date regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                then str_to_date(s.session_date, '%Y-%m-%d') end),
       max(case when s.avg_attention_duration regexp '^[0-9]+(\\.[0-9]+)?$'
                then cast(s.avg_attention_duration as decimal(6,2)) end),
       max(case when s.response_time regexp '^[0-9]+(\\.[0-9]+)?$'
                then cast(s.response_time as decimal(6,2)) end),
       max(case when s.task_completion_rate regexp '^[0-9]+(\\.[0-9]+)?$'
                 and cast(s.task_completion_rate as decimal(10,2)) between 0 and 100
                then cast(s.task_completion_rate as decimal(5,2)) end),
       max(case when s.engagement_score regexp '^[0-9]+(\\.[0-9]+)?$'
                 and cast(s.engagement_score as decimal(10,2)) between 0 and 100
                then cast(s.engagement_score as decimal(5,2)) end),
       max(case when s.number_of_attention_shifts regexp '^[0-9]+$'
                then cast(s.number_of_attention_shifts as signed) end)
from   child_engagement_analytics_raw.engagement s
where  s.session_id regexp '^[0-9]+$'
  and  s.child_id regexp '^[0-9]+$'
  and  s.activity_id regexp '^[0-9]+$'
  and  cast(s.child_id as unsigned)    in (select child_id    from child_engagement_analytics.child)
  and  cast(s.activity_id as unsigned) in (select activity_id from child_engagement_analytics.activity)
group by cast(s.session_id as unsigned), cast(s.child_id as unsigned), cast(s.activity_id as unsigned);
