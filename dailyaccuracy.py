import pandas as pd
import string
import datetime
from database import MssqlHandler
from tools.gchat_logging import send_to_g_chat

READ_SQL_CXN = MssqlHandler("r")
from sentence_transformers import SentenceTransformer, util

model = SentenceTransformer('all-MiniLM-L6-v2')

class DailyAccuracy:
    def __init__(self):
        self.current_date = datetime.date.today()
        self.previous_day = self.current_date - datetime.timedelta(days=1)

    def data(self, categoryname, brandid):
        READ_SQL_CXN.execute(f"""Select MAIN1.BrandID as Brandid, Ait.Response_Text as ResponseGenie,Main.Description as AgentReply
        From  {categoryname}.dbo.Tag_{categoryname} MAIN1 WITH(NOLOCK)
        Left Join {categoryname}.dbo.Tag_{categoryname} MAIN WITH(NOLOCK) on MAIN1.BrandID = MAIN.BrandID  
        AND (Case when MAIN.ChannelGroupID in (7) Then Convert(Varchar(200),MAIN.PostID) ELSE Main.PostSocialID END)
            =(Case when MAIN1.ChannelGroupID in (7) Then MAIN1.ObjectID ELSE MAIN1.TweetIDorFbid  END) AND MAIN.IsBrandPost=1
        LEFT OUTER JOIN {categoryname}.dbo.mstAISuggestedTokenDetails Ait WITH(Nolock) on  Ait.TagID=Main1.Tagid
        Where MAIN1.IsBrandPost=0  AND MAIN1.ExistingTagID>0 
        AND MAIN1.BrandID= {brandid}  
        AND MAIN1.CreatedDate >='2023-09-24 18:30:00' AND MAIN1.CreatedDate <='2023-09-27 18:29:59'
          """)
        ans = READ_SQL_CXN.fetch_df()
        return ans

    def operations(self, df):
        # drop duplicates
        df = df.drop_duplicates()
        # Remove rows containing the common "ssre reply"
        common_ssre_reply = df['AgentReply'].mode()[0]
        df = df[df['AgentReply'] != common_ssre_reply]
        # drop na from AgentReply and ResponseGenie
        df = df.dropna(subset=['AgentReply'])
        df = df.dropna(subset=['ResponseGenie'])
        # remove twitter handles
        df['ResponseGenie'] = df['ResponseGenie'].str.replace(r'@\w+', '', regex=True)
        df['AgentReply'] = df['AgentReply'].str.replace(r'@\w+', '', regex=True)
        # remove #tages
        df['ResponseGenie'] = df['ResponseGenie'].str.replace(r'#\w+', '', regex=True)
        df['AgentReply'] = df['AgentReply'].str.replace(r'#\w+', '', regex=True)
        # Remove punctuation
        df['ResponseGenie'] = df['ResponseGenie'].str.replace(f'[{string.punctuation}]', '', regex=True)
        df['AgentReply'] = df['AgentReply'].str.replace(f'[{string.punctuation}]', '', regex=True)
        return df


    def calculate_sentence_similarity(self, sentence1, sentence2):
        # Encode the sentences into embeddings
        embedding1 = model.encode(sentence1, convert_to_tensor=True)
        embedding2 = model.encode(sentence2, convert_to_tensor=True)

        # Calculate cosine similarity
        cosine_similarity = util.pytorch_cos_sim(embedding1, embedding2)

        return cosine_similarity.item()

    def accuracy_update(self):
        try:
            subset_df = pd.read_csv("Response_Genie_brands.csv")
            df1 = subset_df.head(4)
            msg = f"date {self.current_date}"
            for index, row in df1.iterrows():
                try:
                    res = self.data(row['categoryname'], row['BrandID'])
                    brand_name = row['BrandName']
                    res = self.operations(res)
                    total_response = len(res)
                    score = []
                    for i, j in zip(res["ResponseGenie"].values, res["AgentReply"].values):
                        score.append(self.calculate_sentence_similarity(str(i), str(j)))
                    res["score"] = score
                    average = res['score'].mean()
                    threshold = 0.8
                    # Count rows with 'score' greater than or equal to the threshold
                    count_above_threshold = len(res[res['score'] >= threshold])
                    accuracy = count_above_threshold / total_response
                    text = f"Brand: {brand_name} Avg Similarity Score: {average} {total_response} accuracy: {accuracy}"
                    msg += '\n' + text

                except Exception as e:
                    print(f"An error occurred for Category: {row['categoryname']}, BrandID: {row['BrandID']}")
                    print(f"Error: {str(e)}")
                    continue
            send_to_g_chat(data=msg)
        except Exception as e:
            print(f"An error occurred while processing the CSV file: {str(e)}")