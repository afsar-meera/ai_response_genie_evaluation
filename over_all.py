import datetime
import string
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer, util
from nltk.translate.bleu_score import sentence_bleu
from database import MssqlHandler
from tools.gchat_logging import send_to_g_chat
from sheet_link import create_editable_sheet

model = SentenceTransformer('all-MiniLM-L6-v2')
READ_SQL_CXN = MssqlHandler("r")


class DailyAccuracy:
    def __init__(self):
        self.current_date = datetime.date.today()
        self.previous_day = self.current_date - datetime.timedelta(days=1)
        self.three_days_ago = self.current_date - datetime.timedelta(days=3)
        self.formatted_date = self.previous_day.strftime("%dth %b'%y")

    def operations(self, df):
        # Drop duplicates
        df = df.drop_duplicates()
        # Drop NA from AgentReply and ResponseGenie
        df = df.dropna(subset=['AgentReply'])
        df = df.dropna(subset=['ResponseGenie'])

        # Check if there is a mode for 'AgentReply'
        if not df['AgentReply'].empty:
            common_ssre_reply = df['AgentReply'].mode()[0]
            # Remove rows containing the common "ssre reply"
            df = df[df['AgentReply'] != common_ssre_reply]

        # Drop NA from AgentReply and ResponseGenie
        df = df.dropna(subset=['AgentReply'])
        df = df.dropna(subset=['ResponseGenie'])

        # Remove Twitter handles and hashtags
        df['ResponseGenie'] = df['ResponseGenie'].str.replace(r'@\w+', '', regex=True)
        df['AgentReply'] = df['AgentReply'].str.replace(r'@\w+', '', regex=True)
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

    def calculate_bleu_score(self, reference, candidate):
        reference_tokens = reference.split()
        candidate_tokens = candidate.split()
        return sentence_bleu([reference_tokens], candidate_tokens)

    def categorize_response(self, score):
        if score >= 0.9:
            return "Exact Response"
        elif score >= 0.7:
            return "Minor Change in the Response"
        elif score >= 0.5:
            return "Change in Response - Same meaning"
        else:
            return "Suggestion not used"


    def agent_response_count(self, category_name, brand_id):
        READ_SQL_CXN.execute(f"""SELECT ExistingTagID,TagID , channeltype, Brandid, Categoryid, authorid, Channelgroupid
        FROM {category_name}.dbo.tag_{category_name} WITH (NOLOCK)
        WHERE Brandid={brand_id}
        AND  ISNULL(Isdeleted,0)=0  AND ISNULL(ExistingTagID,0)>0
        AND CONVERT(Date,Dateadd(Minute,330,CreatedDate)) >= '{self.previous_day}'
        AND CONVERT(Date,Dateadd(Minute,330,CreatedDate)) <= '{self.current_date}';""")
        df = READ_SQL_CXN.fetch_df()
        return len(df)

    def flr(self,category_name, brandid):
        READ_SQL_CXN.execute(f""";With CTE as (
        Select TagID,TicketID,BrandID
        From (
        Select Row_Number() Over (Partition by Object_CaseID Order by RespondedDateTime ) as RN, 
        MRT.TagID,Object_CaseID as TicketID ,MAIN.BrandID
        From {category_name}.dbo.MstResponsetimedetails  MRT with(Nolock)
        INNER JOIN {category_name}.dbo.tag_{category_name} MAIN With(Nolock) ON MAIN.ExistingTagID=MRT.Object_CaseID and MRT.BrandID=Main.BrandID
        AND MRT.TagID=Main.TagID
        Where Main.BrandID={brandid}  AND MRT.Object_status=3
        AND CONVERT(Date,Dateadd(Minute,330,Main.CreatedDate)) >= '{self.previous_day}'
        AND CONVERT(Date,Dateadd(Minute,330,Main.CreatedDate)) <= '{self.current_date}'
        )x
        Where RN=1
        )

        Select Sum(FirstLevelTAT_Seconds)/Count(Distinct CaseID) as AVG_FLR_ai
        From {category_name}.dbo.MstCasedetails CD With(Nolock)
        INNER JOIN {category_name}.dbo.tag_{category_name} MAIN With(Nolock) ON MAIN.ExistingTagID=CD.CaseID and CD.BrandID=Main.BrandID
        Where CD.Brandid ={brandid}
         AND MAIN.TagID in (
         Select ai.TagID from {category_name}.Dbo.MstAIsuggestedTokendetails ai With(Nolock)
         INNER JOIN CTE ct ON ct.TicketID= ai.ticket_id AND ai.TagID=ct.TagID AND ct.BrandID=ai.BrandID
         )""")
        df = READ_SQL_CXN.fetch_df()
        return df.iloc[0, 0]


    def brand_genie_data(self):
        READ_SQL_CXN.execute(""" SELECT 
        mc.categoryname,
        mb.BrandName,
        mb.BrandID
        FROM mstBrands_metadata bm
        INNER JOIN mstbrands mb ON bm.brandid = mb.brandid
        INNER JOIN mstcategories mc ON mb.categoryid = mc.id
        WHERE IsSuggestedResponseEnabled = 1
        AND ISNULL(isbrandactive, 0) = 1
        AND ISNULL(iscategoryactive, 0) = 1 """)
        df = READ_SQL_CXN.fetch_df()
        return df

    def get_tag_info(self, tag_id, category_name):
        try:
            READ_SQL_CXN.execute(f"""
               SELECT ExistingTagID, channeltype, Brandid, Categoryid, authorid, Channelgroupid
               FROM {category_name}.dbo.tag_{category_name} WITH (NOLOCK)
               WHERE tagid = {tag_id};
               """)
            df_ = READ_SQL_CXN.fetch_df()
            if not df_.empty:
                return df_.iloc[0]
        except:
            pass
        return [None, None, None, None, None, None]

    def data(self, category_name, brand_name, brandid):
        READ_SQL_CXN.execute(f"""
                SELECT DATEADD(MINUTE, 330, InsertedDate) AS date,Response_Text as ResponseGenie, *
                FROM {category_name}.dbo.mstAISuggestedTokenDetails with (NOlock)
                WHERE CONVERT(DATE, DATEADD(MINUTE, 330, InsertedDate)) = '{self.previous_day}'
                and Brandid={brandid}
                ORDER BY date DESC;
                """
                             )

        df = READ_SQL_CXN.fetch_df()

        tag_info_columns = ["ExistingTagID", "channeltype", "Brandid", "Categoryid", "authorid", "Channelgroupid"]
        df[tag_info_columns] = df["TagID"].apply(lambda x: pd.Series(self.get_tag_info(x, category_name)))

        df["AgentReply"] = ""

        for i in df[(df["ai_feature_type"] == 2)].index:
            try:
                READ_SQL_CXN.execute(f"""EXEC LB3_GetUserCommunicationHistory_V51
                        @CategoryName = '{category_name}',
                        @BrandName = '{brand_name}',
                        @EndDate = '{self.current_date} 18:29:59',
                        @AuthorID = '{df["authorid"][i]}',
                        @ChannelGroupID = {int(df["Channelgroupid"][i])},
                        @TicketID = {int(df["ExistingTagID"][i])},
                        @From = 1,
                        @To = 50,
                        @IsActionableData = 0,
                        @CurrentUserID = NULL;""")
                # print(ticket_query)

                df_ = READ_SQL_CXN.fetch_df()
                df_sorted = df_.sort_values(by='CreatedDate', ascending=False)
                df_sorted = df_sorted.reset_index(drop=True)
                # Added assignment to the line below
                index = df_sorted[df_sorted["TagID"] == df["TagID"][i]].index[0] - 1
                df.loc[i, "AgentReply"] = df_sorted["Description"][index]
            except:
                df.loc[i, "AgentReply"] = None

        for i in df[(df["ai_feature_type"] == 0)].index:
            try:
                READ_SQL_CXN.execute(f"""EXEC LB3_GetUserCommunicationHistory_V51
                        @CategoryName = '{category_name}',
                        @BrandName = '{brand_name}',
                        @EndDate = '{self.current_date} 18:29:59',
                        @AuthorID = '{df["authorid"][i]}',
                        @ChannelGroupID = {int(df["Channelgroupid"][i])},
                        @TicketID = {int(df["ExistingTagID"][i])},
                        @From = 1,
                        @To = 50,
                        @IsActionableData = 0,
                        @CurrentUserID = NULL;""")
                # print(ticket_query)

                df_ = READ_SQL_CXN.fetch_df()
                df_sorted = df_.sort_values(by='CreatedDate', ascending=False)
                df_sorted = df_sorted.reset_index(drop=True)
                # Added assignment to the line below
                index = df_sorted[df_sorted["TagID"] == df["TagID"][i]].index[0]
                df.loc[i, "AgentReply"] = df_sorted["Description"][index]
            except:
                df.loc[i, "AgentReply"] = None
        return df


    def alert_formatting(self):
        try:
            brand_df = self.brand_genie_data()
            # data1 = {'categoryname': ['TataDigitalDb'], 'BrandName': ['TATADigital'], 'BrandID': [6954]}
            # brand_df = pd.DataFrame(data1)
            total_response, kb = [], []
            all_genie_agent_per, flr_count = [], []
            exact_res, minor_change, same_meaning , not_used = [], [], [], []
            for index, row in brand_df.iterrows():
                try:
                    agent_count = self.agent_response_count(row['categoryname'], row['BrandID'])

                    flr_count.append(self.flr(row['categoryname'], row['BrandID']))
                    res1 = self.data(row['categoryname'], row['BrandName'], row['BrandID'])
                    res = self.operations(res1)
                    responses = len(res)
                    if agent_count != 0:
                        genie_agent_per = (responses/agent_count)*100
                    else:
                        genie_agent_per = np.nan
                    total_response.append(responses)
                    all_genie_agent_per.append(f'{genie_agent_per:.2f}')

                    if responses > 0:
                        true_caution = res['caution'].sum()
                        false_caution = responses - true_caution
                        # out_of_kb.append(true_caution)
                        kb.append(f'{(false_caution/responses)*100:.2f}')
                        exact, minor, same_mean, no_use = 0, 0, 0, 0
                        for i, j, k in zip(res["ResponseGenie"].values, res["AgentReply"].values,
                                           res["caution"].values):
                            score = self.calculate_sentence_similarity(str(i), str(j))
                            if score >= 0.9:
                                exact+=1
                            elif score >= 0.7:
                                minor +=1
                            elif score >= 0.5:
                                same_mean +=1
                            else:
                                no_use +=1

                        exact_res.append(f'{(exact/responses)*100:.2f}')
                        minor_change.append(f'{(minor/responses)*100:.2f}')
                        same_meaning.append(f'{(same_mean/responses)*100:.2f}')
                        not_used.append(f'{(no_use/responses)*100:.2f}')

                    else:
                        kb.append(None)
                        exact_res.append(None)
                        minor_change.append(None)
                        same_meaning.append(None)
                        not_used.append(None)

                except Exception as e:
                    print(f"An error occurred for Category: {row['categoryname']}, BrandID: {row['BrandID']}")
                    print(f"Error: {str(e)}")
                    continue

            brand_df["Total Suggested Response"] = total_response
            brand_df["% of Sug VS total replies"] = all_genie_agent_per
            brand_df["FLR TAT of tickets where Response are suggested "] = flr_count
            brand_df["% sug form KB"] = kb
            brand_df["% Exact Response "] = exact_res
            brand_df["% with minor change "] = minor_change
            brand_df["% with same meaning "] = same_meaning
            brand_df["% not used"] = not_used

            return brand_df.sort_values(by='Total Response', ascending=False)
            # return brand_df
        except Exception as e:
            print(f"An error occurred while processing Alerts: {str(e)}")

    def accuracy_update(self):
        final_df = self.alert_formatting()
        final_df = final_df.fillna('').astype(str)
        print(final_df)
        final_df.to_csv("accu1.csv")
        # sheet_name = f"Genie Accuracy Alert {self.current_date}"
        # link = create_editable_sheet(final_df, sheet_name)
        # text = f'Date: {self.current_date}\nPlease find Daily Genie Accuracy Alert\nLink: {link}'
        # send_to_g_chat(data=text)



if __name__ == '__main__':
    controller = DailyAccuracy()
    controller.accuracy_update()