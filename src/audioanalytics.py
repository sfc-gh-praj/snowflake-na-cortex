import json
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark import types as T
from snowflake.snowpark import Row, Column
from snowflake.snowpark.functions import table_function
import pandas as pd


def init_data(session: Session) -> str:
  
  """
    Loads the Tables required for the Streamlit App 
    Args:
      session (Session): An active session object for authentication and communication.

    Returns:
      str: A status message indicating the result of the provisioning process.
   """
  # session.file.put('../data/Sample_Audio_Text.csv', '@app_public.data_stage')

  session.call('code_schema.copy_file','/data/Sample_Audio_Text.csv', '@app_public.data_stage/Sample_Audio_Text.csv')

  sp_df=session.read.options({"INFER_SCHEMA":True,"PARSE_HEADER":True,"FIELD_OPTIONALLY_ENCLOSED_BY":'"'}).csv('@app_public.data_stage/Sample_Audio_Text.csv')
  sp_df.write.mode("append").save_as_table("app_public.ALL_CLAIMS_RAW")
  generated_transcripts_df = session.table("app_public.ALL_CLAIMS_RAW")
  # oneshort_prompted_snowdf = generated_transcripts_df.with_column("Oneshort_PROMPT", F.call_udf("code_schema.get_qa_prompt_oneshort", F.col("CONVERSATION")))

  # Referring the UDTF created in setup.sql
  udtf_table_function = table_function("code_schema.get_qa_prompt_oneshort")

  # Joining UDTF with ALL_CLAIMS_RAW table
  oneshort_prompted_snowdf=generated_transcripts_df.join_table_function(udtf_table_function(F.col("conversation")).over(partition_by=["datetime","AUDIOFILE","CONVERSATION","PRESIGNED_URL_PATH","DURATION"]).alias('Oneshort_PROMPT'))
 
  # Pass the LLAMA prompt column to the COMPLETE function
  oneshort_info_extracted_snowdf = oneshort_prompted_snowdf.with_column("EXTRACTED_INFO", F.call_builtin("SNOWFLAKE.CORTEX.COMPLETE", F.lit('llama2-70b-chat'), F.col("ONESHORT_PROMPT")))

  oneshort_info_extracted_snowdf.write.mode('append').save_as_table("app_public.TRANSCRIPT_INFO_EXTRACTED_QA")


  # Extracting the required columns using cortex llm functions and loading into the variant column name call_details as json. Also we are getting the call summary and sentiments using cortex llm functions

  session.sql(f"""
          insert into app_public.AUDIO_CLAIMS_EXTRACTED_INFO(
                                                  datetime,
                                                  audio_file_name,       
                                                  audio_full_file_path,
                                                  raw_conversation,
                                                  prompted_conversation,
                                                  duration,
                                                  call_details,
                                                  call_summary,
                                                  call_sentiment
                                              )
          with audio_details as
          (
              select datetime, 
                      audiofile, 
                      presigned_url_path, 
                      conversation,
                      extracted_info,
                      duration,
                      try_parse_json(SNOWFLAKE.CORTEX.COMPLETE('llama3-8b',concat('<s>[INST] Analyze the audio transcripts found between the tags <question></question> tags and generate only the requested json output. Do not include any other language before or after the requested output.Include any policy number or claim number with values like policy number POL_NotAvailable or claim number CL_NotAvailable. Do not include the prompt. Output should only be json and if  ClaimNumber is not found then assign the value as NotFound in double quotes and if there is no PolicyNumber found then assign the value as NotFound in double quotes. 
                      Provide a valid JSON response which can be parsed by Snowflake using the following format, no preamble. Collision with animal cannot be a intent . Below is the JSON that should be the output:
                      {{ Representative: string,
                      Customer:string, 
                      ClaimNumber : string, 
                      PolicyNumber : string,
                      CallIntent:string,
                      CallToAction:string,
                      Issue:string,
                      Resolution:string,
                      NextSteps:string,
                      ModeofUpdate:string,
                      PurposeOfCall:string,
                      ResponseMode:string,
                      FirstCallResolution:float,
                      CallQuality: string,
                      Net Promoter Score :int}}. 
              
                      Intent of the call should be in 2 words. Provide if it was a first call resolution as a score values ranging between 0.01 and 0.99.
                      Call Quality involves monitoring and evaluating the quality of interactions based on communication skills, adherence to protocols, and overall customer handling and values will be Good,Average,Poor. 
                      Net Promoter Score value between 10 -100 and also consider the resolution response time and skill of the Representative in handling the issue.
                      
                      Below pattern of conversations is not a first call resolution :
                      Owen: Hello, this is Owen from AutoAssure Insurance. How can I assist you today?
                      Jessica: Hi, Owen. My name is Jessica Turner, and I am calling to check the status of my insurance claim for a recent car accident.
                      Owen: I am here to help, Jessica. Can you please provide me with your policy number so I can access your information?
                      Jessica: It is POL901234.
                      Owen: Thank you, Jessica. Let me pull up the details of your claim. While I am doing that, is there something specific you are unclear about regarding the status?
                      Jessica: Yes, Owen. I had an accident two weeks ago, and I have not received any updates on the claim.
                      Owen: I understand your concern, Jessica. I will investigate the status of your claim and provide you with an update. You will receive an email shortly.
                      Jessica: Yes, Owen. How soon can I expect to hear back about the status of my claim?
                      Owen: I appreciate your patience, Jessica. Our claims department is actively working on your case, and you can expect an update within the next 48 hours.
                      Jessica: Thank you, Owen. I will be waiting for the update. You haveve been very helpful.
                      Owen: It is my pleasure, Jessica. If you have any more questions or concerns, feel free to contact us.
                      Jessica: Goodbye.
                      
                      Only provide JSON in your response. Dont provide any explanations.Do not prefix "Here is the JSON response for the conversation" for the json data generated. Value for the Claim Number should be NotFound in double quotes if Claim Number or Policy Number is not found in the text between the <question></question> for all rows.','<question>'
                      ,EXTRACTED_INFO,'</question> [/INST] Answer: Here is the JSON response for the conversation')
                      )
                      ) as call_details 
                      ,SNOWFLAKE.CORTEX.SUMMARIZE(concat('Provide the summary for the conversation in <con> </con> tag.Generate only the requested output. 
                      Do not include any other language before or after the requested output.Also include any policy number or claim number if found the conversation. Do not include the prompt. Dont provide any explanations.
                      <con>',CONVERSATION,'</con>')) as call_summary
                      , SNOWFLAKE.CORTEX.SENTIMENT(CONVERSATION) as call_sentiment
          from app_public.TRANSCRIPT_INFO_EXTRACTED_QA
          )
          select * from audio_details

""").collect()
  




  return 'Loaded Data for the App'


