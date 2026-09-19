USE child_engagement_analytics;
SET GLOBAL local_infile = 1;

-- organization
LOAD DATA LOCAL INFILE 'C:\Users\OMAR YASSER\Desktop\NTI\org.csv'
INTO TABLE organization
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(org_id, name, type);

-- preferences
LOAD DATA LOCAL INFILE 'C:\Users\OMAR YASSER\Desktop\NTI\preferences.csv'
INTO TABLE preferences
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(preference_id, name, category);

-- child
LOAD DATA LOCAL INFILE 'C:\Users\OMAR YASSER\Desktop\NTI\child.csv'
INTO TABLE child
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(child_id, fname, lname, @working_hours, @age, family_size, num_of_siblings,
 household_income, access_to_technology, number_of_devices, @access_tech2,
 @school_type, @environment_type, @fuzzy_key)
SET
    parents_working_hours = NULLIF(@working_hours, ''),
    age = NULLIF(@age, ''),
    school_type = @school_type,
    environment_type = @environment_type;

-- activity (activity_type column removed from the sheet, not used anymore)
LOAD DATA LOCAL INFILE 'C:\Users\OMAR YASSER\Desktop\NTI\activity.csv'
INTO TABLE activity
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(activity_id, org_id, @activity_name, duration, group_size, interaction_type,
 movement_required, @difficulty_level, participation_type, @activity_format)
SET
    name = @activity_name,
    difficulty_level = @difficulty_level,
    activity_format = @activity_format;

-- lifestyle
LOAD DATA LOCAL INFILE 'C:\Users\OMAR YASSER\Desktop\NTI\life_style.csv'
INTO TABLE lifestyle
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(child_id, sleep_duration, screen_time, gaming, social_media_usage,
 outdoor_time, study_time, free_play, @daily_total, @unit_flag, @excess, @tech_flag);

-- child_organization
LOAD DATA LOCAL INFILE 'C:\Users\OMAR YASSER\Desktop\NTI\child_org.csv'
INTO TABLE child_organization
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(child_id, org_id, enrollment_date);

-- engagement
LOAD DATA LOCAL INFILE 'C:\Users\OMAR YASSER\Desktop\NTI\engagement.csv'
INTO TABLE engagement
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(session_id, child_id, activity_id, session_date, @shifts, @response_time,
 @completion_rate, @attention_duration, @eng_score)
SET
    number_of_attention_shifts = @shifts,
    response_time = @response_time,
    task_completion_rate = @completion_rate,
    avg_attention_duration = NULLIF(@attention_duration, ''),
    engagement_score = @eng_score;

-- child_preference
LOAD DATA LOCAL INFILE 'C:\Users\OMAR YASSER\Desktop\NTI\child_preferences.csv'
INTO TABLE child_preference
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(child_id, preference_id);

SET FOREIGN_KEY_CHECKS = 1;


-- 1) هل الطفل اللي بيكمل النشاط بنسبة أعلى بيبقى تركيزه أعلى؟ (Pearson correlation بين task_completion_rate وengagement_score)
SELECT
    (COUNT(*)*SUM(task_completion_rate*engagement_score) - SUM(task_completion_rate)*SUM(engagement_score))
    / SQRT( (COUNT(*)*SUM(POW(task_completion_rate,2)) - POW(SUM(task_completion_rate),2))
          * (COUNT(*)*SUM(POW(engagement_score,2)) - POW(SUM(engagement_score),2)) ) AS correlation
FROM engagement
WHERE task_completion_rate IS NOT NULL AND engagement_score IS NOT NULL;

-- 2) هل زيادة عدد مرات تشتت الانتباه بتقلل الـengagement؟ (correlation بين number_of_attention_shifts وengagement_score)
SELECT
    (COUNT(*)*SUM(number_of_attention_shifts*engagement_score) - SUM(number_of_attention_shifts)*SUM(engagement_score))
    / SQRT( (COUNT(*)*SUM(POW(number_of_attention_shifts,2)) - POW(SUM(number_of_attention_shifts),2))
          * (COUNT(*)*SUM(POW(engagement_score,2)) - POW(SUM(engagement_score),2)) ) AS correlation
FROM engagement
WHERE number_of_attention_shifts IS NOT NULL AND engagement_score IS NOT NULL;

-- 3) هل سرعة استجابة الطفل مرتبطة بمستوى الـengagement بتاعه؟ (correlation بين response_time وengagement_score)
SELECT
    (COUNT(*)*SUM(response_time*engagement_score) - SUM(response_time)*SUM(engagement_score))
    / SQRT( (COUNT(*)*SUM(POW(response_time,2)) - POW(SUM(response_time),2))
          * (COUNT(*)*SUM(POW(engagement_score,2)) - POW(SUM(engagement_score),2)) ) AS correlation
FROM engagement
WHERE response_time IS NOT NULL AND engagement_score IS NOT NULL;

-- 4) هل الطفل اللي بيحافظ على انتباهه لفترة أطول بيحقق engagement أعلى؟ (correlation بين avg_attention_duration وengagement_score)
SELECT
    (COUNT(*)*SUM(avg_attention_duration*engagement_score) - SUM(avg_attention_duration)*SUM(engagement_score))
    / SQRT( (COUNT(*)*SUM(POW(avg_attention_duration,2)) - POW(SUM(avg_attention_duration),2))
          * (COUNT(*)*SUM(POW(engagement_score,2)) - POW(SUM(engagement_score),2)) ) AS correlation
FROM engagement
WHERE avg_attention_duration IS NOT NULL AND engagement_score IS NOT NULL;

-- 5) نسبة الجلسات Low/Medium/High إزاي موزعة (الحدود: أقل من 40 = Low، من 40 لـ70 = Medium، فوق 70 = High)
SELECT
    CASE
        WHEN engagement_score < 40 THEN 'Low'
        WHEN engagement_score < 70 THEN 'Medium'
        ELSE 'High'
    END AS engagement_band,
    COUNT(*) AS num_sessions,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM engagement WHERE engagement_score IS NOT NULL), 1) AS pct
FROM engagement
WHERE engagement_score IS NOT NULL
GROUP BY engagement_band
ORDER BY FIELD(engagement_band, 'Low','Medium','High');

-- 6) هل الأنشطة اللي محتاجة حركة جسدية بتاخد engagement أعلى من اللي مش محتاجة؟
SELECT a.movement_required, ROUND(AVG(e.engagement_score),2) AS avg_engagement, COUNT(*) AS num_sessions
FROM engagement e
JOIN activity a ON e.activity_id = a.activity_id
GROUP BY a.movement_required;

-- 7) هل صعوبة النشاط (Low/Medium/High) بتأثر على متوسط الـengagement؟
SELECT a.difficulty_level, ROUND(AVG(e.engagement_score),2) AS avg_engagement, COUNT(*) AS num_sessions
FROM engagement e
JOIN activity a ON e.activity_id = a.activity_id
GROUP BY a.difficulty_level
ORDER BY FIELD(a.difficulty_level, 'Low','Medium','High');

-- 8) هل شكل النشاط (Digital/Physical/Hybrid) بيفرق في متوسط الـengagement؟
SELECT a.activity_format, ROUND(AVG(e.engagement_score),2) AS avg_engagement, COUNT(*) AS num_sessions
FROM engagement e
JOIN activity a ON e.activity_id = a.activity_id
GROUP BY a.activity_format
ORDER BY avg_engagement DESC;

-- 9) هل نوع المؤسسة (مدرسة/حضانة/نادي...) بيأثر على متوسط الـengagement اللي بيحصل جواها؟
SELECT o.type, ROUND(AVG(e.engagement_score),2) AS avg_engagement, COUNT(*) AS num_sessions
FROM engagement e
JOIN activity a ON e.activity_id = a.activity_id
JOIN organization o ON a.org_id = o.org_id
GROUP BY o.type
ORDER BY avg_engagement DESC;

-- 10) هل نوع المشاركة (Voluntary/Assigned/Opt-in/Mandatory) بيفرق في متوسط الـengagement؟
SELECT a.participation_type, ROUND(AVG(e.engagement_score),2) AS avg_engagement, COUNT(*) AS num_sessions
FROM engagement e
JOIN activity a ON e.activity_id = a.activity_id
GROUP BY a.participation_type
ORDER BY avg_engagement DESC;
