drop database if exists child_engagement_analytics;
create database child_engagement_analytics;
use child_engagement_analytics;

create table organization (
    org_id int primary key auto_increment,
    name varchar(100) not null,
    type varchar(50)
);

create table child (
    child_id int primary key auto_increment,
    fname varchar(50),
    lname varchar(50),
    age int check (age between 3 and 18),
    family_size int,
    num_of_siblings int,
    parents_working_hours decimal(4,1),
    household_income decimal(10,2),
    access_to_technology boolean,
    number_of_devices int,
    environment_type varchar(20),
    school_type varchar(20)
);

create table child_organization (
    child_id int not null,
    org_id int not null,
    enrollment_date date,
    primary key (child_id, org_id),
    foreign key (child_id) references child(child_id),
    foreign key (org_id) references organization(org_id)
);

create table activity (
    activity_id int primary key auto_increment,
    org_id int not null,
    name varchar(100) not null,
    duration int,
    group_size int,
    interaction_type varchar(20),
    activity_format varchar(20),
    participation_type varchar(20),
    difficulty_level varchar(20),
    movement_required boolean,
    foreign key (org_id) references organization(org_id)
);

create table preferences (
    preference_id int primary key auto_increment,
    name varchar(50) not null,
    category varchar(50)
);

create table lifestyle (
    child_id int primary key,
    sleep_duration decimal(3,1),
    screen_time decimal(3,1),
    gaming decimal(3,1),
    social_media_usage decimal(3,1),
    outdoor_time decimal(3,1),
    study_time decimal(3,1),
    free_play decimal(3,1),
    foreign key (child_id) references child(child_id)
);

create table engagement (
    session_id int primary key auto_increment,
    child_id int not null,
    activity_id int not null,
    session_date date,
    avg_attention_duration decimal(6,2),
    response_time decimal(6,2),
    task_completion_rate decimal(5,2),
    engagement_score decimal(5,2),
    number_of_attention_shifts int,
    foreign key (child_id) references child(child_id),
    foreign key (activity_id) references activity(activity_id),
    check (task_completion_rate between 0 and 100),
    check (engagement_score between 0 and 100)
);

create table child_preference (
    child_id int not null,
    preference_id int not null,
    primary key (child_id, preference_id),
    foreign key (child_id) references child(child_id),
    foreign key (preference_id) references preferences(preference_id)
);
