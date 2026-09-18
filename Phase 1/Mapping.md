organization :

 org_id (pk)  ,  name , type
--------------------------------------------------------------------------------------------------------------------------------
activity :

 activity_id (pk), org_id (fk) -> organization(org_id) , name, duration,group_size, interaction_type, activity_format,
 participation_type, difficulty_level, movement_required 
--------------------------------------------------------------------------------------------------------------------------------
child :

 child_id (pk), fname, lname, age, family_size, num_of_siblings, parents_working_hours, household_income, access_to_technology      
  number_of_devices,  environment_type, school_type 
--------------------------------------------------------------------------------------------------------------------------------
life_style :

 child_id (pk) (fk) -> child(child_id), sleep_duration, screen_time, gaming, social_media_usage, outdoor_time, study_time, free_play
--------------------------------------------------------------------------------------------------------------------------------
preferences :

 preference_id (pk), name,  category
--------------------------------------------------------------------------------------------------------------------------------
engagement  :   
       
  session_id (pk) , child_id (fk) -> child(child_id) , activity_id (fk) -> activity(activity_id) ,avg_attention_duration ,response_time                  
  task_completion_rate ,  engagement_score ,number_of_attention_shifts , session_date

--------------------------------------------------------------------------------------------------------------------------------
child_preference :

  child_id (fk) ->  child(child_id) ,preference_id (fk) -> preferences(preference_id) , PRIMARY KEY (child_id, preference_id)   

--------------------------------------------------------------------------------------------------------------------------------

child_organization :

 child_id (fk) ->  child(child_id) ,  org_id (fk) -> organization(org_id) , enrollment_date ,
 PRIMARY KEY (child_id, org_id)

--------------------------------------------------------------------------------------------------------------------------------
