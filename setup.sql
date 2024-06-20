CREATE SCHEMA IF NOT EXISTS core;
CREATE APPLICATION ROLE IF NOT EXISTS app_user;

CREATE SCHEMA IF NOT EXISTS app_public;
GRANT USAGE ON SCHEMA app_public TO APPLICATION ROLE app_user;

CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE app_user;

GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_user;


CREATE STAGE IF NOT EXISTS app_public.data_stage
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
GRANT READ ON STAGE app_public.data_stage TO APPLICATION ROLE app_user;
GRANT WRITE ON STAGE app_public.data_stage TO APPLICATION ROLE app_user;


CREATE or REPLACE TABLE app_public.ALL_CLAIMS_RAW (
        DATETIME DATE,
        AUDIOFILE VARCHAR(16777216),
        CONVERSATION VARCHAR(16777216),
        PRESIGNED_URL_PATH VARCHAR(16777216),
        DURATION FLOAT 
);

create or replace TABLE app_public.TRANSCRIPT_INFO_EXTRACTED_QA (
	DATETIME DATE,
	AUDIOFILE VARCHAR(16777216),
	CONVERSATION VARCHAR(16777216),
	PRESIGNED_URL_PATH VARCHAR(16777216),
	DURATION FLOAT ,
	ONESHORT_PROMPT VARCHAR(16777216),
	EXTRACTED_INFO VARCHAR(16777216)
);

CREATE OR REPLACE TABLE app_public.AUDIO_CLAIMS_EXTRACTED_INFO 
    (
        DATETIME DATE,
        AUDIO_FILE_NAME VARCHAR(100),
        AUDIO_FULL_FILE_PATH VARCHAR(16777216),
        RAW_CONVERSATION VARCHAR(16777216),
        PROMPTED_CONVERSATION VARCHAR(16777216),
        DURATION FLOAT,
        CALL_DETAILS VARIANT,
        CALL_SUMMARY VARCHAR(16777216),
        CALL_SENTIMENT FLOAT,
        REPONSE_GIVEN FLOAT
    );

CREATE OR REPLACE PROCEDURE core.copy_file(src varchar, dst varchar)
    RETURNS STRING
    LANGUAGE python
    RUNTIME_VERSION = 3.8
    HANDLER = 'handler'
    PACKAGES = ('snowflake-snowpark-python')
AS $$
def handler(session, src, dst):
    return session.file.put_stream(session.file.get_stream(src), dst, overwrite=True, parallel=1, auto_compress=False)
$$
;
GRANT USAGE ON PROCEDURE core.copy_file(varchar,varchar) TO APPLICATION ROLE app_user;

CREATE OR REPLACE PROCEDURE code_schema.load_data()
  RETURNS string
  LANGUAGE python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python', 'requests')
  imports = ('/src/audioanalytics.py')
  handler = 'audioanalytics.init_data';

GRANT USAGE ON PROCEDURE code_schema.load_data() TO APPLICATION ROLE app_user;


-- CREATE or REPLACE FUNCTION code_schema.get_qa_prompt_oneshort(input VARCHAR(16777216))
--   RETURNS VARCHAR(16777216)
--   LANGUAGE PYTHON
--   RUNTIME_VERSION=3.8
--   packages = ('pandas')
--   IMPORTS = ('/src/udf_prompt.py')
--   HANDLER='udf_prompt.get_qa_prompt_oneshort';


-- GRANT USAGE ON FUNCTION code_schema.get_qa_prompt_oneshort(VARCHAR) TO APPLICATION ROLE app_user;

GRANT SELECT,INSERT,UPDATE,DELETE ON app_public.AUDIO_CLAIMS_EXTRACTED_INFO TO APPLICATION ROLE app_user;


GRANT SELECT,INSERT,UPDATE,DELETE ON app_public.TRANSCRIPT_INFO_EXTRACTED_QA TO APPLICATION ROLE app_user;

GRANT SELECT,INSERT,UPDATE,DELETE ON app_public.ALL_CLAIMS_RAW TO APPLICATION ROLE app_user;

-- GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO APPLICATION ROLE app_user;

create or replace function code_schema.get_qa_prompt_oneshort(raw_text varchar)
returns table (text_prompt varchar)
language python
runtime_version=3.8
packages=('pandas')
handler='Prompt'
as $$
from _snowflake import vectorized
import re
import pandas

class Prompt:
    @vectorized(input=pandas.DataFrame)
    def end_partition(self,df):
        df.columns=['RAW_TEXT']
        df['LLAMA_PROMPT'] = df['RAW_TEXT'].apply(lambda transcript: f""" <s>[INST]
<<SYS>>
You are an expert in analyzing customer transcripts and tasked with answering questions about an article.  
Please ensure that your responses are socially unbiased and positive in nature.
If you don't know the answer, please don't share false information.

Output answer in the following format and no preamble. :
                                        * <Representative> : text of the Representative
                                        * <Customer>  : text of the customer
Extract the name of the Representative name from the transcript 
and replace that name for <Representative> and extract Customer name from the transcript replace that with <Customer> for creating the conversation. 

Below is one example output for the conversation:   
                                                                    
Representative: Jason
Customer: Michelle Turner

Jason: Hi, Michelle. My name is Jason from AutoAssure Insurance. How may I assist you today?
Michelle: Hi, Jason. I'm calling regarding a dispute I have with a recent claim.
Jason: Of course, Michelle. Can you please provide me with your claim number so I can access your file?
Michelle: Certainly, it's CLM876543.
Jason: Thank you, Michelle. Let me retrieve your claim information. While I'm doing that, could you explain the nature of the dispute?
Michelle: Of course, Jason. I believe there's been a misunderstanding in the assessment of the damages to my vehicle.
Jason: I appreciate you providing details, Michelle. I'll review your claim thoroughly and work towards a resolution.
Michelle: Thank you, Jason. I hope we can reach a fair resolution.
Jason: We'll do our best, Michelle. I'll keep you updated on the progress. Have a good day!
                                                                              
<</SYS>>                                                                                                             
We need to analyze the text of the transcript found in <transcript> </transcript> tag as conversation between representative and the customer. Convert the transcript in the format mentioned in the example. 
Extract the name of the Representative name from the transcript and replace that name for <Representative> and extract Customer name from the transcript replace that with <Customer> while creating the conversation.                                       
Do not include "Sure, here is the conversation between in the output. Never say thank you, that you are happy to help, that you are an AI agent, etc. Just answer directly". 
<transcript>
    {transcript}
</transcript> 

[/INST] Answer: Sure, I would be happy to help! """)
        return df[['LLAMA_PROMPT']]
$$;



GRANT USAGE ON FUNCTION code_schema.get_qa_prompt_oneshort(VARCHAR) TO APPLICATION ROLE app_user;

-- CREATE or REPLACE FUNCTION code_schema.get_qa_prompt_oneshort(ques string)
-- returns string
-- language python
-- runtime_version = 3.8
-- packages = ('pandas')
-- handler = 'prompt'
-- as $$
-- import pandas 

-- def prompt(query:str):
--     response_series =  f""" <s>[INST]
-- <<SYS>>
-- You are a expert in analyzing customer transcripts and tasked with answering questions about an article.  
-- Please ensure that your responses are socially unbiased and positive in nature.
-- If you don't know the answer, please don't share false information.

-- Output answer in the following format and no preamble. :
--                                         * <Representative> : text of the Representative
--                                         * <Customer>  : text of the customer
-- Extract the name of the Representative name from the transcript 
-- and replace that name for <Representative> and extract Customer name from the trasnscript replace that with <Customer> for creating the conversation. 

-- Below is one example output for the conversation:   
                                                                    
-- Representative: Jason
-- Customer: Michelle Turner

-- Jason: Hi, Michelle. My name is Jason from AutoAssure Insurance. How may I assist you today?
-- Michelle: Hi, Jason. I'm calling regarding a dispute I have with a recent claim.
-- Jason: Of course, Michelle. Can you please provide me with your claim number so I can access your file?
-- Michelle: Certainly, it's CLM876543.
-- Jason: Thank you, Michelle. Let me retrieve your claim information. While I'm doing that, could you explain the nature of the dispute?
-- Michelle: Of course, Jason. I believe there's been a misunderstanding in the assessment of the damages to my vehicle.
-- Jason: I appreciate you providing details, Michelle. I'll review your claim thoroughly and work towards a resolution.
-- Michelle: Thank you, Jason. I hope we can reach a fair resolution.
-- Jason: We'll do our best, Michelle. I'll keep you updated on the progress. Have a good day!
                                                                              
-- <</SYS>>                                                                                                             
-- We need to analyze the text of the transcript found in <transcript> </transcript> tag as conversation between representative and the customer. Convert the transcript in the format mentioned in the example. 
-- Extract the name of the Representative name from the transcript and replace that name for <Representative> and extract Customer name from the trasnscript replace that with <Customer> while creating the conversation.                                       
-- Do not include "Sure, here is the conversation between in the output. Never say thank you, that you are happy to help, that you are an AI agent, etc. Just answer directly". 
-- <transcript>
--      {query}
-- </transcript> 


-- [/INST] Answer: Sure, I would be happy to help! """                                        
                                        
--     return response_series
-- $$;




